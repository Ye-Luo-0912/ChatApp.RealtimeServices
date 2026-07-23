# NATS Trust Boundary (P1-7)

RealtimeServices no longer treats payload `SenderUserId` / history `UserId` as authoritative
when gateway identity enforcement is enabled.

## What landed in code

1. **Gateway identity headers** (shared contract in Abstractions + Integration):
   - `X-Chat-User-Id`
   - `X-Chat-Session-Id`
2. Integration helper: `RealtimeIntegrationTelemetry.CreateIdentityHeaders(userId, sessionId)`.
3. Consumers extract headers into envelope `TrustedUserId` / `TrustedSessionId`.
4. Workers reject permanent failures when `RequireGatewayIdentity` is on and:
   - header is missing, or
   - header user/session does not match payload fields.
5. **NATS client auth wiring**: `Nats:Auth` supports Username/Password, Token, CredsFile, NKey/Seed/NKeyFile.
   Integration/Gateway 同样支持 `RealtimeIntegration:Auth`（`NatsRealtimeMessageBus`）。
6. Production defaults: `RequireGatewayIdentity` defaults to **true** outside Development.
7. Production validation **requires both**:
   - `Nats:Auth` credentials (CredsFile / NKeyFile / Username / Token / Seed / NKey) — transport auth
   - `Nats:Trust:RequireGatewayIdentity=true` — secondary payload/header consistency check only  
   Gateway identity headers **cannot** substitute for NATS account authentication. Also apply subject ACLs on the NATS server.
8. Sample accounts/ACL config: [`nats-accounts.sample.conf`](./nats-accounts.sample.conf).

## Gateway publish contract (required for Production)

When publishing incoming messages / receipts / history queries, inject authenticated identity:

```csharp
var headers = RealtimeIntegrationTelemetry.CreateIdentityHeaders(
    authenticatedUserId,
    authenticatedSessionId);
// pass headers into JetStream publish / NATS request
```

Do **not** trust client-supplied sender fields alone.

## Remaining ops steps (NATS server)

These are infrastructure steps outside the .NET process:

1. Enable NATS **accounts** (or user/pass) for gateway vs realtime-services vs admin.
2. Apply **subject ACLs**, e.g.:
   - gateway account: publish `chat.incoming-messages`, `chat.message-receipts`, `chat.message-history.query`; subscribe `chat.realtime-events.*`
   - realtime-services account: subscribe incoming/receipts/history; publish events + dead-letters
3. Mount credentials into containers (`Nats__Auth__CredsFile` or username/password).
4. Prefer mTLS between NATS clients and servers in Production.
5. Keep JetStream replicas >= 3 in non-dev (already enforced by host validation).

Local docker-compose remains unauthenticated for developer ergonomics; set
`Nats__Trust__RequireGatewayIdentity=false` only for local smoke tests that omit headers.
