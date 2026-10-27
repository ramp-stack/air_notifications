# AIR Notifications

AIR contracts for application push configuration and device registrations, with an optional
push delivery server. This crate does not depend on Maverick OS, Prism, or RAMP.

Applications and deployment tools use the library without features. RAMP converts its native
Maverick `PushToken` into this crate's `PushDestination` and drives `Notifications` through
its ordinary shared AIR instance services. The library supports APNs and FCM destinations.

Version 0.2 uses AIR 4 and AIR Server 2. The push server stores its account catalog under the
`air` cache key. On its first start with this version, it reconstructs that catalog by replaying
the existing encrypted inbox and storage channels; contract registrations retain their IDs.

## Client policy

`Subscription::new(instance_id)` owns an instance's notification preference and whether it has
caught up to the channel head. `notify(enabled, subscription)` returns the actions needed when
that preference changes. `response(output, received_file, subscription)` processes AIR outputs
and returns `Action::Local(header, body)` or `Action::Remote(Notification)` values for the runtime
to execute. Pass the current subscription before preparing the instance's next request.

Historical replay does not alert. After reaching the channel head, incoming reads alert and
acknowledge their file; confirmation of a local write only acknowledges it. Subscription outputs
arm the next watch or remove it when notifications are disabled. `start()` resets catch-up state
while preserving the preference. Construct a fresh `Subscription` when restoring an instance;
this local policy is not serialized into the notification contract.

`Notifications::changes(message)` checks whether a contract message would change the current
state. Runtimes use it before sending redundant watches or stale acknowledgments. Contract
application uses the same check after verifying the signer. Token registration, task scheduling,
native alert presentation, and sending AIR messages remain responsibilities of the runtime.

## Contracts

`NotificationApplication` is owned by the developer's AIR identity. Initialize it with
`(bundle_id, sandbox)`, send `ApnsCredentials { key, key_id, team_id }`, and share its instance
with `Notifications::server()`. `key` is the PKCS#8 PEM text from Apple's `.p8` file. Only the
owner can update credentials. Sandbox and production use separate application instances.
Keep the credentials and private contract location on the deployment tool and server.

For Android, create `FcmApplication` with `(firebase_project_id, android_package_name)`,
send `FcmCredentials { private_key, private_key_id, client_email }`, and share it with the same
`Notifications::server()`. These fields come from a Google service-account JSON file. Give that
account the Firebase Cloud Messaging API Admin role (`roles/firebasecloudmessaging.admin`) on
the target project and enable the Firebase Cloud Messaging API. The Android package must match
the Firebase app and APK. Only the contract owner can rotate credentials. Each provider has its
own application instance; Apple uses `NotificationApplication`, Android uses `FcmApplication`.

`Notifications` is initialized with `(installation_id, application_id)` and owned by the
installation's AIR identity. Its owner sends `Destination`, `Watch`, `Received`, and `Remove`.
`Watch` registers an instance ID and its next `(public_file_key, air_server_name)`.
`Received` acknowledges that exact file without removing a newer watch.

All contracts use the existing AIR `Contract` interface. The APNs application and registration
contract IDs remain unchanged. FCM adds a separate application contract and a destination
variant. Update the server before registering Android destinations. The default server's AIR name is
stored in `src/server.name`.

## Server

```sh
cargo run --features server
```

Only the server module and executable require this feature. It enables the APNs and FCM HTTP clients,
credentials signing, and delivery persistence dependencies. The server builds without UI,
native hardware, or FFmpeg.

The process prints its AIR name at startup and receives registrations through AIR. It has no
inbound public listener. Identity, inbox, application configurations, pending deliveries, and
completed watches are stored in `NOTIFICATIONS.db`; `NOTIFICATION_STORAGE` selects another
SQLite file. Retain this database across restarts to preserve its identity. After application
configuration, it contains private signing keys and must remain private.

One server supports multiple developers and applications. Each registration uses its referenced
application's provider and credentials, including the APNs topic/environment or FCM project/package. Only confirmed contract updates
affect delivery. Invalid configurations are logged without stopping other applications.
No global provider credentials or additional Android server are required.

When a watched file appears, the server waits 500 ms before sending the fixed alert
**You have a notification.** Remote contracts cannot set a custom title or message. An owner
acknowledgment cancels pending delivery. Late or lost acknowledgments can still produce a
duplicate. A watch covers one file; the application must register the next file after catching up.

APNs and FCM temporary failures retry. FCM honors Retry-After, backs off with jitter, refreshes
OAuth access tokens, and pauses permanent failures until credentials or the destination change. Invalid destinations pause that registration until its destination
or application configuration changes. A crash after Apple accepts an alert but before completion
is saved can repeat it; stable APNs collapse IDs reduce queued duplicates. Existing AIR
session and subscription limits apply. FCM uses a stable Android notification tag to replace
retries of the same alert in the tray. Notification delivery is best effort, not exactly once.
Real Apple delivery requires valid Apple credentials, matching
entitlements, the correct sandbox/production environment, and a device token.

Install the standalone server from crates.io with:

```sh
cargo install air_notifications --features server --locked
```

The server uses published AIR dependencies. Run it with a persistent `NOTIFICATION_STORAGE`
path so its identity and registrations survive restarts.

## Android delivery

The same server sends Android alerts through the FCM HTTP v1 API. It obtains short-lived OAuth
tokens from the application-scoped RSA service-account key. The private key is never compiled
into the Android app, included in a device registration, or printed by credential Debug output.

Maverick's Android bridge packages the Firebase Messaging SDK. A configured app gets its current
FCM token and sends replacements through RAMP's existing push service. The server sends a
high-priority **notification** payload, so Firebase displays it when the application is in the
background, including when its ordinary process has been reclaimed. Foreground messages enter
Maverick's `Input::Notification` and RAMP's normal notification event path.

Android notification permission must be granted. A user force-stop prevents delivery until the
app is opened again; it is distinct from backgrounding or ordinary process death. Google Play
services or a Google APIs emulator are required for this transport. No Firebase database,
Cloud Functions deployment, or per-application custom server is needed.

See [FCM Android setup](https://firebase.google.com/docs/cloud-messaging/android/get-started),
[background delivery](https://firebase.google.com/docs/cloud-messaging/android/receive-messages),
and [HTTP v1 authentication](https://firebase.google.com/docs/cloud-messaging/send/v1-api).
