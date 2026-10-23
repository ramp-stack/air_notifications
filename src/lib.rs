use air::{Contract, Id, Metadata, Name, PublicKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter, Result as FmtResult};

#[cfg(feature = "server")]
pub mod server;

pub type File = (PublicKey, Name);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PushDestination { Apns(String), Fcm(String), }

#[derive(Clone, Hash, Serialize, Deserialize)]
pub struct ApnsCredentials {
    pub key: String,
    pub key_id: String,
    pub team_id: String,
}
impl Debug for ApnsCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ApnsCredentials")
            .field("key_id", &self.key_id)
            .field("team_id", &self.team_id)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct NotificationApplication {
    pub owner: Name,
    pub topic: String,
    pub sandbox: bool,
    pub credentials: Option<ApnsCredentials>,
}
impl Contract for NotificationApplication {
    type Init = (String, bool);
    type Message = ApnsCredentials;
    type Result = bool;

    fn id() -> Id { Id::hash("ramp.notification.application.v1") }
    fn init((topic, sandbox): Self::Init, metadata: Metadata) -> Self {
        Self { owner: metadata.signer, topic, sandbox, credentials: None }
    }
    fn apply(&mut self, credentials: ApnsCredentials, metadata: Metadata) -> bool {
        if metadata.signer != self.owner { return false; }
        self.credentials = Some(credentials);
        true
    }
}

#[derive(Clone, Hash, Serialize, Deserialize)]
pub struct FcmCredentials {
    pub private_key: String,
    pub private_key_id: String,
    pub client_email: String,
}
impl Debug for FcmCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("FcmCredentials")
            .field("private_key_id", &self.private_key_id)
            .field("client_email", &self.client_email)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct FcmApplication {
    pub owner: Name,
    pub project: String,
    pub package: String,
    pub credentials: Option<FcmCredentials>,
}
impl Contract for FcmApplication {
    type Init = (String, String);
    type Message = FcmCredentials;
    type Result = bool;

    fn id() -> Id { Id::hash("ramp.notification.fcm.application.v1") }
    fn init((project, package): Self::Init, metadata: Metadata) -> Self {
        Self { owner: metadata.signer, project, package, credentials: None }
    }
    fn apply(&mut self, credentials: FcmCredentials, metadata: Metadata) -> bool {
        if metadata.signer != self.owner { return false; }
        self.credentials = Some(credentials);
        true
    }
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub enum Notification {
    Destination(PushDestination),
    Watch(Id, PublicKey, Name),
    Received(Id, PublicKey, Name),
    Remove(Id),
}

#[derive(Clone, Debug, Hash, Serialize, Deserialize)]
pub struct Notifications {
    pub owner: Name,
    pub application: Id,
    pub destination: Option<PushDestination>,
    pub files: BTreeMap<Id, File>,
}
impl Contract for Notifications {
    type Init = (String, Id);
    type Message = Notification;
    type Result = bool;

    fn id() -> Id { Id::hash("ramp.notifications.v2") }
    fn init((_installation, application): Self::Init, metadata: Metadata) -> Self {
        Self { owner: metadata.signer, application, destination: None, files: BTreeMap::new() }
    }
    fn apply(&mut self, message: Notification, metadata: Metadata) -> bool {
        if metadata.signer != self.owner { return false; }
        match message {
            Notification::Destination(destination) => {
                if self.destination.as_ref() == Some(&destination) { return false; }
                self.destination = Some(destination);
                true
            }
            Notification::Watch(instance, key, server) => {
                self.files.insert(instance, (key, server)) != Some((key, server))
            }
            Notification::Received(instance, key, server) => {
                if self.files.get(&instance) != Some(&(key, server)) { return false; }
                self.files.remove(&instance);
                true
            }
            Notification::Remove(instance) => self.files.remove(&instance).is_some(),
        }
    }
}

impl Notifications {
    pub fn server() -> Name { include_str!("server.name").trim().parse().unwrap() }
}
