use air::Id;
use base64::{ Engine, engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}, };
use ring::{ rand::{SecureRandom, SystemRandom}, signature::{RsaKeyPair, RSA_PKCS1_SHA256}, };
use tokio::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::FcmCredentials;
use super::Delivery;

pub struct Fcm {
    client: reqwest::Client,
    key: RsaKeyPair,
    key_id: String,
    email: String,
    package: String,
    endpoint: String,
    authorization: Mutex<(u64, String)>,
}
impl Fcm {
    pub fn new(project: &str, package: &str, credentials: &FcmCredentials) -> Result<Self, String> {
        if project.is_empty()
            || !project.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || package.is_empty()
            || !package
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'_')
            || credentials.private_key_id.is_empty()
            || credentials.client_email.is_empty()
        { return Err("FCM project, Android package, key ID and client email are required".into()); }
        let encoded: String =
            credentials.private_key.lines().filter(|line| !line.starts_with("-----")).collect();
        let key = STANDARD.decode(encoded).map_err(|_| "Invalid FCM key encoding")?;
        let key = RsaKeyPair::from_pkcs8(&key).map_err(|_| "Invalid FCM RSA private key")?;
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            key,
            key_id: credentials.private_key_id.clone(),
            email: credentials.client_email.clone(),
            package: package.to_owned(),
            endpoint: format!("https://fcm.googleapis.com/v1/projects/{project}/messages:send"),
            authorization: Mutex::new((0, String::new())),
        })
    }

    async fn authorization(&self) -> Option<String> {
        let mut saved = self.authorization.lock().await;
        let mut delay = Duration::from_secs(1);
        loop {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if !saved.1.is_empty() && now + 60 < saved.0 { return Some(saved.1.clone()); }
            let header = serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": self.key_id});
            let claims = serde_json::json!({
                "iss": self.email, "scope": "https://www.googleapis.com/auth/firebase.messaging",
                "aud": "https://oauth2.googleapis.com/token", "iat": now, "exp": now + 3600,
            });
            let unsigned = format!(
                "{}.{}",
                URL_SAFE_NO_PAD.encode(header.to_string()),
                URL_SAFE_NO_PAD.encode(claims.to_string())
            );
            let mut signature = vec![0; self.key.public().modulus_len()];
            self.key
                .sign(&RSA_PKCS1_SHA256, &SystemRandom::new(), unsigned.as_bytes(), &mut signature)
                .unwrap();
            let assertion = format!("{unsigned}.{}", URL_SAFE_NO_PAD.encode(signature));
            let response = self
                .client
                .post("https://oauth2.googleapis.com/token")
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                    ("assertion", &assertion),
                ])
                .send()
                .await;
            let retry = match response {
                Ok(response) if response.status().is_success() => {
                    match response.json::<serde_json::Value>().await {
                        Ok(value) => {
                            if let (Some(token), Some(expires)) =
                                (value["access_token"].as_str(), value["expires_in"].as_u64())
                                && !token.is_empty() && expires > 60
                            {
                                *saved = (now.saturating_add(expires), token.to_owned());
                                return Some(token.to_owned());
                            }
                            eprintln!("FCM OAuth returned an invalid token response");
                        }
                        Err(_) => eprintln!("FCM OAuth response could not be read"),
                    }
                    delay
                }
                Ok(response) => {
                    let status = response.status();
                    if status.as_u16() != 429 && !status.is_server_error() {
                        let error = response.json::<serde_json::Value>().await.unwrap_or_default();
                        let reason = error["error"].as_str().unwrap_or("unknown");
                        eprintln!("FCM OAuth rejected credentials: {status} {reason}");
                        return None;
                    }
                    eprintln!("FCM OAuth temporarily unavailable: {status}");
                    Self::retry_after(response.headers()).max(delay)
                }
                Err(error) => {
                    eprintln!("FCM OAuth connection failed: {}", error.without_url());
                    delay
                }
            };
            Self::wait(retry).await;
            delay = (delay * 2).min(Duration::from_secs(300));
        }
    }

    pub async fn send(&self, token: &str, id: Id) -> Delivery {
        if token.is_empty() { return Delivery::Invalid; }
        let payload = serde_json::json!({"message": {
            "token": token,
            "notification": {"title": "New activity", "body": "You have a notification."},
            "android": {
                "priority": "HIGH", "restricted_package_name": self.package,
                "notification": {"channel_id": "maverick", "tag": id.to_string()},
            },
        }});
        let mut refreshed = false;
        let mut delay = Duration::from_secs(1);
        loop {
            let Some(authorization) = self.authorization().await else { return Delivery::Blocked };
            let response = self
                .client
                .post(&self.endpoint)
                .bearer_auth(&authorization)
                .json(&payload)
                .send()
                .await;
            let retry = match response {
                Ok(response) if response.status().is_success() => return Delivery::Sent,
                Ok(response) => {
                    let status = response.status();
                    if status.as_u16() == 401 && !refreshed {
                        let mut saved = self.authorization.lock().await;
                        if saved.1 == authorization { saved.1.clear(); }
                        refreshed = true;
                        continue;
                    }
                    if status.as_u16() != 429 && !status.is_server_error() {
                        let error = response.json::<serde_json::Value>().await.unwrap_or_default();
                        let details = error["error"]["details"].as_array()
                            .map(Vec::as_slice).unwrap_or_default();
                        let reason = details.iter().find(|detail| {
                            detail["@type"] == "type.googleapis.com/google.firebase.fcm.v1.FcmError"
                        }).and_then(|detail| detail["errorCode"].as_str());
                        let invalid_token = details.iter().any(|detail| {
                            detail["@type"] == "type.googleapis.com/google.rpc.BadRequest"
                                && detail["fieldViolations"].as_array().is_some_and(|violations| {
                                    violations.iter().any(|item| item["field"] == "message.token")
                                })
                        });
                        eprintln!(
                            "FCM rejected notification for {}: {status} {}", self.package,
                            reason.or_else(|| error["error"]["status"].as_str()).unwrap_or("unknown")
                        );
                        return if reason == Some("UNREGISTERED") || invalid_token { Delivery::Invalid }
                            else { Delivery::Blocked };
                    }
                    if status.as_u16() == 429 { delay = delay.max(Duration::from_secs(60)); }
                    eprintln!(
                        "FCM temporarily rejected notification for {}: {status}",
                        self.package
                    );
                    Self::retry_after(response.headers()).max(delay)
                }
                Err(error) => { eprintln!("FCM connection failed: {}", error.without_url()); delay }
            };
            Self::wait(retry).await;
            delay = (delay * 2).min(Duration::from_secs(300));
        }
    }

    fn retry_after(headers: &reqwest::header::HeaderMap) -> Duration {
        let Some(value) = headers.get("retry-after").and_then(|value| value.to_str().ok()) else {
            return Duration::ZERO;
        };
        if let Ok(seconds) = value.parse::<u64>() { return Duration::from_secs(seconds); }
        httpdate::parse_http_date(value)
            .ok()
            .and_then(|date| date.duration_since(SystemTime::now()).ok())
            .unwrap_or_default()
    }
    async fn wait(delay: Duration) {
        let mut random = [0; 2];
        SystemRandom::new().fill(&mut random).unwrap();
        let jitter = Duration::from_millis(u16::from_ne_bytes(random) as u64 % 1000);
        tokio::time::sleep(delay.saturating_add(jitter)).await;
    }
}
