use air::{Contract, Id, Metadata, Name, Output, PublicKey};
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

#[derive(Clone, Debug)]
pub enum Action {
    Local(&'static str, &'static str),
    Remote(Notification),
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
        if metadata.signer != self.owner || !self.changes(&message) {return false;}
        match message {
            Notification::Destination(destination) => self.destination = Some(destination),
            Notification::Watch(instance, key, server) => {
                self.files.insert(instance, (key, server));
            }
            Notification::Received(instance, _, _) | Notification::Remove(instance) => {
                self.files.remove(&instance);
            }
        }
        true
    }
}

impl Notifications {
    pub fn server() -> Name { include_str!("server.name").trim().parse().unwrap() }
    pub fn changes(&self, message: &Notification) -> bool {
        match message {
            Notification::Destination(destination) => self.destination.as_ref() != Some(destination),
            Notification::Watch(instance, key, server) => {
                self.files.get(instance) != Some(&(*key, *server))
            }
            Notification::Received(instance, key, server) => {
                self.files.get(instance) == Some(&(*key, *server))
            }
            Notification::Remove(instance) => self.files.contains_key(instance),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Subscription {
    instance: Id,
    live: bool,
    enabled: bool,
}
impl Subscription {
    pub fn new(instance: Id) -> Self {Self {instance, live: false, enabled: false}}
    pub fn enabled(&self) -> bool {self.enabled}
    pub fn start(&mut self) {self.live = false;}
    pub fn notify(&mut self, enabled: bool, subscription: Option<File>) -> Vec<Action> {
        self.enabled = enabled;
        if !enabled {return vec![Action::Remote(Notification::Remove(self.instance))]}
        if self.live && let Some((key, server)) = subscription {
            return vec![Action::Remote(Notification::Watch(self.instance, key, server))]
        }
        vec![]
    }
    pub fn response<R>(&mut self, output: &Output<R>, received: File, subscription: Option<File>) -> Vec<Action> {
        let mut actions = vec![];
        if self.enabled && self.live {
            if matches!(output, Output::Read(_, _)) {
                actions.push(Action::Local("New activity", "You have a notification."));
            }
            if matches!(output, Output::Read(_, _) | Output::Created(_, _)) {
                actions.push(Action::Remote(Notification::Received(
                    self.instance, received.0, received.1,
                )));
            }
        }
        if matches!(output, Output::Subscribed) {
            self.live = true;
            if let Some((key, server)) = subscription {
                let message = if self.enabled {Notification::Watch(self.instance, key, server)}
                    else {Notification::Remove(self.instance)};
                actions.push(Action::Remote(message));
            }
        }
        actions
    }
}
