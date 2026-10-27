use ::air::{
    Air, Contract, DefaultResolver, Id, Instance, Location, Name, Request, RequestType, Resolver,
    Response, Secret,
};
use air_server::Client;
use base64::{ Engine, engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD}, };
use crossfire::{AsyncRx, spsc};
use futures::{StreamExt, stream::FuturesUnordered};
use ring::{ rand::SystemRandom, signature::{EcdsaKeyPair, ECDSA_P256_SHA256_FIXED_SIGNING}, };
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{Mutex as AsyncMutex, OnceCell, watch};
use tokio::task::{AbortHandle, JoinSet};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::{
    ApnsCredentials, FcmApplication, File, NotificationApplication, Notifications, PushDestination,
};

mod fcm;
use fcm::Fcm;

type Responses = AsyncRx<spsc::List<Response>>;

pub struct NotificationServer {
    secret: Secret,
    air: Air,
    cache: Cache,
    network: Arc<Network>,
    providers: watch::Sender<HashMap<Id, Arc<Provider>>>,
    resolver: DefaultResolver,
    streams: HashMap<RequestType, Responses>,
    registrations: JoinSet<()>,
    locations: BTreeSet<Id>,
}
impl NotificationServer {
    pub async fn start() {
        let path =
            std::env::var_os("NOTIFICATION_STORAGE").unwrap_or_else(|| "NOTIFICATIONS.db".into());
        let secret = air_server::Server::secret(Path::new(&path));
        println!("Notification server: {}", secret.name());
        let cache = Cache::new(Path::new(&path));
        let air = cache.load("air").unwrap_or_else(|| Air::new(secret.clone()).unwrap());
        let server = Self {
            secret,
            air,
            cache,
            network: Arc::new(Network::default()),
            providers: watch::channel(HashMap::new()).0,
            resolver: DefaultResolver::default(),
            streams: HashMap::new(),
            registrations: JoinSet::new(),
            locations: BTreeSet::new(),
        };
        server.run().await;
    }

    async fn run(mut self) {
        for contract in [NotificationApplication::id(), FcmApplication::id(), Notifications::id()] {
            for location in self.air.list(&contract) { self.register(location); }
        }
        let requests = self.air.start();
        self.cache.save("air", &self.air);
        for (kind, request) in requests {
            let responses = self.network.send(Name::orange_me(), request).await;
            self.streams.insert(kind, responses);
        }
        loop {
            let incoming = async {
                let mut pending: FuturesUnordered<_> = self
                    .streams
                    .iter_mut()
                    .map(|(kind, stream)| async move { (*kind, stream.recv().await.unwrap()) })
                    .collect();
                pending.next().await.unwrap()
            };
            let (kind, response) = tokio::select! {
                event = incoming => event,
                stopped = self.registrations.join_next(), if !self.registrations.is_empty() => {
                    panic!("Notification registration stopped: {stopped:?}");
                }
            };
            if !matches!(response, Response::Read(_, None)) { self.streams.remove(&kind); }
            let location = self.air.response(&mut self.resolver, kind, response).await;
            let requests = self.air.requests();
            self.cache.save("air", &self.air);
            if let Some(location) = location { self.register(location); }
            for (kind, request) in requests {
                let responses = self.network.send(Name::orange_me(), request).await;
                self.streams.insert(kind, responses);
            }
        }
    }

    fn register(&mut self, location: Location) {
        if ![Notifications::id(), NotificationApplication::id(), FcmApplication::id()]
            .contains(&location.contract)
            || !self.locations.insert(Id::hash(&location))
        { return; }
        let id = Id::hash(&location);
        if location.contract == NotificationApplication::id() {
            self.application::<NotificationApplication>(location);
            return;
        }
        if location.contract == FcmApplication::id() {
            self.application::<FcmApplication>(location);
            return;
        }
        let state = self.cache.load(&id.to_string()).unwrap_or_else(|| RegistrationState {
            instance: Instance::receive(self.secret.clone(), location).unwrap(),
            ready: BTreeSet::new(),
            completed: BTreeSet::new(),
            inactive: false,
        });
        let registration = Registration {
            id,
            state,
            cache: self.cache.clone(),
            network: self.network.clone(),
            providers: self.providers.clone(),
            jobs: HashMap::new(),
            tasks: JoinSet::new(),
        };
        self.registrations.spawn(registration.run());
    }
    fn application<C: ApplicationConfiguration>(&mut self, location: Location) {
        let instance = self
            .cache
            .load(&Id::hash(&location).to_string())
            .unwrap_or_else(|| Instance::<C>::receive(self.secret.clone(), location).unwrap());
        let application = Application {
            instance,
            cache: self.cache.clone(),
            network: self.network.clone(),
            providers: self.providers.clone(),
        };
        self.registrations.spawn(application.run());
    }
}

trait ApplicationConfiguration: Contract {
    fn provider(&self) -> Result<Option<Provider>, String>;
}
impl ApplicationConfiguration for NotificationApplication {
    fn provider(&self) -> Result<Option<Provider>, String> {
        self.credentials
            .as_ref()
            .map(|credentials| {
                Apns::new(&self.topic, self.sandbox, credentials).map(Provider::Apns)
            })
            .transpose()
    }
}
impl ApplicationConfiguration for FcmApplication {
    fn provider(&self) -> Result<Option<Provider>, String> {
        self.credentials
            .as_ref()
            .map(|credentials| {
                Fcm::new(&self.project, &self.package, credentials).map(Provider::Fcm)
            })
            .transpose()
    }
}

enum Event { Available(File), Delivered(File, PushDestination, bool), }

#[derive(Serialize, Deserialize)]
struct RegistrationState {
    instance: Instance<Notifications>,
    ready: BTreeSet<File>,
    completed: BTreeSet<File>,
    inactive: bool,
}

#[derive(Clone)]
struct Cache(Arc<Mutex<Connection>>);
impl Cache {
    fn new(path: &Path) -> Self {
        let db = Connection::open(path).unwrap();
        db.busy_timeout(Duration::from_secs(5)).unwrap();
        db.pragma_update(None, "journal_mode", "WAL").unwrap();
        db.pragma_update(None, "synchronous", "FULL").unwrap();
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS notifications(key TEXT PRIMARY KEY, value BLOB NOT NULL);",
        )
        .unwrap();
        Self(Arc::new(Mutex::new(db)))
    }
    fn load<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let bytes: Option<Vec<u8>> = self
            .0
            .lock()
            .unwrap()
            .query_row("SELECT value FROM notifications WHERE key = ?1", [key], |row| row.get(0))
            .optional()
            .unwrap();
        bytes.map(|bytes| serde_json::from_slice(&bytes).unwrap())
    }
    fn save<T: Serialize>(&self, key: &str, value: &T) {
        self.0
            .lock()
            .unwrap()
            .execute(
                "INSERT OR REPLACE INTO notifications(key, value) VALUES (?1, ?2)",
                params![key, serde_json::to_vec(value).unwrap()],
            )
            .unwrap();
    }
}

#[derive(Default)]
struct Network(AsyncMutex<HashMap<Name, Arc<OnceCell<Arc<Client>>>>>);
impl Network {
    async fn send(&self, server: Name, request: Request) -> Responses {
        let client = self.0.lock().await.entry(server).or_default().clone();
        let client = client
            .get_or_init(async || {
                let identity = DefaultResolver::default().resolve(server, None).await;
                let mut delay = Duration::from_millis(250);
                loop {
                    match Client::new((*identity).clone(), "wss").await {
                        Ok(client) => return Arc::new(client),
                        Err(error) => eprintln!("AIR connection to {server}: {error}"),
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
            })
            .await;
        client.send(request).await
    }
    async fn available(&self, file: File) {
        let responses = self.send(file.1, Request::Read(file.0)).await;
        loop {
            match responses.recv().await.unwrap() {
                Response::Read(_, Some(_)) => return,
                Response::Read(_, None) => {}
                _ => panic!("Unexpected AIR file response"),
            }
        }
    }
}

struct Registration {
    id: Id,
    state: RegistrationState,
    cache: Cache,
    network: Arc<Network>,
    providers: watch::Sender<HashMap<Id, Arc<Provider>>>,
    jobs: HashMap<File, AbortHandle>,
    tasks: JoinSet<Event>,
}
impl Registration {
    async fn run(mut self) {
        let server = self.state.instance.location().server;
        let request = self.state.instance.start();
        self.save();
        let mut responses = self.network.send(server, request).await;
        let mut resolver = DefaultResolver::default();
        self.schedule();
        let mut providers = self.providers.subscribe();
        let mut configuration = self
            .state
            .instance
            .confirmed()
            .and_then(|state| providers.borrow().get(&state.application).cloned());
        loop {
            tokio::select! {
                changed = providers.changed() => {
                    changed.unwrap();
                    let current = self.state.instance.confirmed()
                        .and_then(|state| providers.borrow().get(&state.application).cloned());
                    let changed = match (&configuration, &current) {
                        (Some(previous), Some(current)) => !Arc::ptr_eq(previous, current),
                        (None, None) => false,
                        _ => true,
                    };
                    if changed { self.state.inactive = false; self.cancel().await; }
                }
                response = responses.recv() => {
                    let previous = self.state.instance.confirmed()
                        .and_then(|state| state.destination.clone());
                    self.state.instance.response(&mut resolver, response.unwrap()).await;
                    let current = self.state.instance.confirmed()
                        .and_then(|state| state.destination.clone());
                    if current != previous { self.state.inactive = false; self.cancel().await; }
                    let active: BTreeSet<File> = self.state.instance.confirmed()
                        .map(|state| state.files.values().copied().collect()).unwrap_or_default();
                    if self.jobs.keys().any(|file| !active.contains(file)) {self.cancel().await;}
                    self.state.ready.retain(|file| active.contains(file));
                    self.state.completed.retain(|file| active.contains(file));
                    let request = self.state.instance.request();
                    self.save();
                    if let Some(request) = request {
                        responses = self.network.send(server, request).await;
                    }
                }
                event = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    match event.unwrap() {
                        Ok(Event::Available(file)) => {
                            self.jobs.remove(&file);
                            self.state.ready.insert(file);
                        }
                        Ok(Event::Delivered(file, destination, delivered)) => {
                            let current = &self.state.instance.confirmed().unwrap().destination;
                            if current.as_ref() != Some(&destination) {continue}
                            self.jobs.remove(&file);
                            if delivered {
                                self.state.ready.remove(&file);
                                self.state.completed.insert(file);
                            } else {
                                self.state.inactive = true;
                                self.cancel().await;
                            }
                        }
                        Err(error) if error.is_cancelled() => continue,
                        Err(error) => panic!("Notification worker failed: {error}"),
                    }
                    self.save();
                }
            }
            self.schedule();
            configuration = self
                .state
                .instance
                .confirmed()
                .and_then(|state| providers.borrow().get(&state.application).cloned());
        }
    }
    async fn cancel(&mut self) {
        self.tasks.abort_all();
        while let Some(task) = self.tasks.join_next().await {
            if let Err(error) = task {
                if !error.is_cancelled() { panic!("Notification worker failed: {error}") }
            }
        }
        self.jobs.clear();
    }
    fn save(&self) { self.cache.save(&self.id.to_string(), &self.state) }
    fn schedule(&mut self) {
        let Some(state) = self.state.instance.confirmed() else { return };
        if self.state.inactive { return; }
        let Some(destination) = state.destination.clone() else { return };
        let providers = self.providers.borrow();
        let Some(provider) = providers.get(&state.application) else { return };
        for file in state.files.values() {
            if self.state.completed.contains(file) || self.jobs.contains_key(file) { continue; }
            let file = *file;
            let task = if self.state.ready.contains(&file) {
                let provider = provider.clone();
                let destination = destination.clone();
                let id = self.id;
                self.tasks.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let delivered = provider.send(&destination, Id::hash(&(id, file))).await;
                    Event::Delivered(file, destination, delivered)
                })
            } else {
                let network = self.network.clone();
                self.tasks.spawn(async move {
                    network.available(file).await;
                    Event::Available(file)
                })
            };
            self.jobs.insert(file, task);
        }
    }
}

struct Application<C: ApplicationConfiguration> {
    instance: Instance<C>,
    cache: Cache,
    network: Arc<Network>,
    providers: watch::Sender<HashMap<Id, Arc<Provider>>>,
}
impl<C: ApplicationConfiguration> Application<C> {
    async fn run(mut self) {
        let id = self.instance.id();
        let server = self.instance.location().server;
        let mut resolver = DefaultResolver::default();
        let mut configured = None;
        let request = self.instance.start();
        self.cache.save(&id.to_string(), &self.instance);
        let mut responses = self.network.send(server, request).await;
        loop {
            self.instance.response(&mut resolver, responses.recv().await.unwrap()).await;
            self.cache.save(&id.to_string(), &self.instance);
            if let Some(application) = self.instance.confirmed() {
                let version = Id::hash(application);
                if configured != Some(version) {
                    configured = Some(version);
                    let provider = match application.provider() {
                        Ok(provider) => provider,
                        Err(error) => {
                            eprintln!("Invalid push configuration for {id}: {error}");
                            None
                        }
                    };
                    self.providers.send_modify(|providers| {
                        providers.remove(&id);
                        if let Some(provider) = provider {
                            providers.insert(id, Arc::new(provider));
                        }
                    });
                }
            }
            if let Some(request) = self.instance.request() {
                responses = self.network.send(server, request).await;
            }
        }
    }
}

enum Provider { Apns(Apns), Fcm(Fcm), }
impl Provider {
    async fn send(&self, destination: &PushDestination, id: Id) -> bool {
        match (self, destination) {
            (Self::Apns(provider), PushDestination::Apns(token)) => provider.send(token, id).await,
            (Self::Fcm(provider), PushDestination::Fcm(token)) => provider.send(token, id).await,
            _ => { eprintln!("Push destination does not match its application's provider"); false }
        }
    }
}

struct Apns {
    client: reqwest::Client,
    key: EcdsaKeyPair,
    key_id: String,
    team: String,
    topic: String,
    endpoint: String,
    authorization: Mutex<(u64, String)>,
}
impl Apns {
    fn new(topic: &str, sandbox: bool, credentials: &ApnsCredentials) -> Result<Self, String> {
        let encoded: String =
            credentials.key.lines().filter(|line| !line.starts_with("-----")).collect();
        let key = STANDARD.decode(encoded).map_err(|_| "Invalid APNs key encoding")?;
        let key =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &key, &SystemRandom::new())
                .map_err(|_| "Invalid APNs P-256 private key")?;
        if topic.is_empty()
            || !topic
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
            || credentials.key_id.is_empty()
            || credentials.team_id.is_empty()
        { return Err("APNs topic, key ID and team ID are required".into()); }
        let endpoint = if sandbox {
            "https://api.sandbox.push.apple.com"
        } else {
            "https://api.push.apple.com"
        };
        Ok(Self {
            client: reqwest::Client::builder()
                .http2_prior_knowledge()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
            key,
            key_id: credentials.key_id.clone(),
            team: credentials.team_id.clone(),
            topic: topic.to_owned(),
            endpoint: endpoint.into(),
            authorization: Mutex::new((0, String::new())),
        })
    }
    fn authorization(&self) -> String {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut saved = self.authorization.lock().unwrap();
        if saved.1.is_empty() || now.saturating_sub(saved.0) >= 50 * 60 {
            let header = serde_json::json!({"alg": "ES256", "kid": self.key_id});
            let claims = serde_json::json!({"iss": self.team, "iat": now});
            let unsigned = format!(
                "{}.{}",
                URL_SAFE_NO_PAD.encode(header.to_string()),
                URL_SAFE_NO_PAD.encode(claims.to_string())
            );
            let signature = self.key.sign(&SystemRandom::new(), unsigned.as_bytes()).unwrap();
            *saved = (now, format!("{unsigned}.{}", URL_SAFE_NO_PAD.encode(signature.as_ref())));
        }
        saved.1.clone()
    }
    async fn send(&self, token: &str, id: Id) -> bool {
        if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) { return false; }
        let payload = serde_json::json!({"aps": {"alert": "You have a notification."}});
        loop {
            let response = self
                .client
                .post(format!("{}/3/device/{token}", self.endpoint))
                .bearer_auth(self.authorization())
                .header("apns-topic", &self.topic)
                .header("apns-push-type", "alert")
                .header("apns-priority", "10")
                .header("apns-collapse-id", id.to_string())
                .json(&payload)
                .send()
                .await;
            match response {
                Ok(response) if response.status().is_success() => return true,
                Ok(response) => {
                    let status = response.status();
                    let retry = response
                        .headers()
                        .get("retry-after")
                        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
                        .unwrap_or(30)
                        .max(1);
                    let body = response.text().await.unwrap_or_default();
                    let reason: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    let reason =
                        reason.get("reason").and_then(|value| value.as_str()).unwrap_or("");
                    if status.as_u16() == 410
                        || matches!(
                            reason,
                            "BadDeviceToken"
                                | "DeviceTokenNotForTopic"
                                | "Unregistered"
                                | "ExpiredToken"
                        )
                    { return false; }
                    if status.as_u16() != 429 && !status.is_server_error() {
                        eprintln!(
                            "APNs rejected a notification for {}: {status} {reason}",
                            self.topic
                        );
                        return false;
                    }
                    eprintln!("APNs temporarily rejected a notification: {status} {reason}");
                    tokio::time::sleep(Duration::from_secs(retry)).await;
                }
                Err(error) => {
                    eprintln!("APNs connection failed: {}", error.without_url());
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
            }
        }
    }
}
