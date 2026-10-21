# AIR Notifications

AIR contracts for application push configuration and device registrations, with an optional
push delivery server. This crate does not depend on Maverick OS, Prism, or RAMP.

Applications and deployment tools use the library without features. RAMP converts its native
Maverick `PushDestination` into this crate's `PushDestination` and drives `Notifications` through
its ordinary shared AIR instance services. The library currently supports APNs destinations.

## Contracts

`NotificationApplication` is owned by the developer's AIR identity. Initialize it with
`(bundle_id, sandbox)`, send `ApnsCredentials { key, key_id, team_id }`, and share its instance
with `Notifications::server()`. `key` is the PKCS#8 PEM text from Apple's `.p8` file. Only the
owner can update credentials. Sandbox and production use separate application instances.
Keep the credentials and private contract location on the deployment tool and server.

`Notifications` is initialized with `(installation_id, application_id)` and owned by the
installation's AIR identity. Its owner sends `Destination`, `Watch`, `Received`, and `Remove`.
`Watch` registers an instance ID and its next `(public_file_key, air_server_name)`.
`Received` acknowledges that exact file without removing a newer watch.

Both contracts use the existing AIR `Contract` interface. Their IDs and serialized format are
unchanged from the original RAMP implementation, so existing registrations, identities, and
SQLite state remain usable after moving the server binary. The default server's AIR name is
stored in `src/server.name`.

## Server

```sh
cargo run --features server
```

Only the server module and executable require this feature. It enables the APNs HTTP client,
credentials signing, and delivery persistence dependencies. The server builds without UI,
native hardware, or FFmpeg.

The process prints its AIR name at startup and receives registrations through AIR. It has no
inbound public listener. Identity, inbox, application configurations, pending deliveries, and
completed watches are stored in `NOTIFICATIONS.db`; `NOTIFICATION_STORAGE` selects another
SQLite file. Retain this database across restarts to preserve its identity. After application
configuration, it contains private signing keys and must remain private.

One server supports multiple developers and applications. Each registration uses its referenced
application's APNs credentials, bundle ID, and environment. Only confirmed contract updates
affect delivery. Invalid configurations are logged without stopping other applications.
No global APNs environment variables are required.

When a watched file appears, the server waits 500 ms before sending the fixed alert
**You have a notification.** Remote contracts cannot set a custom title or message. An owner
acknowledgment cancels pending delivery. Late or lost acknowledgments can still produce a
duplicate. A watch covers one file; the application must register the next file after catching up.

APNs temporary failures retry. Invalid destinations pause that registration until its destination
or application configuration changes. A crash after Apple accepts an alert but before completion
is saved can repeat it; stable APNs collapse IDs reduce queued duplicates. Existing AIR
session and subscription limits apply. Real delivery requires valid Apple credentials, matching
entitlements, the correct sandbox/production environment, and a device token.

Install the standalone server from crates.io with:

```sh
cargo install air_notifications --features server --locked
```

The server uses published AIR dependencies. Run it with a persistent `NOTIFICATION_STORAGE`
path so its identity and registrations survive restarts.
