# Realtime Worker Architecture

The `realtime-worker` acts as the single source of truth for all real-time events, WebSocket communication, and pub/sub messaging across the Rareminds Skill Ecosystem. It is built natively on **Cloudflare Durable Objects** and **Cloudflare Queues** to provide global low-latency state synchronization.

## System Components

### 1. The Proxy/Routing Layer (`index.ts`)
The `fetch` handler serves as a stateless proxy that routes incoming WebSocket connections and handles authentication securely.
- **WebSocket Handshake Validation:** It extracts the user's JWT from the `Sec-WebSocket-Protocol` header.
- **Cross-Worker Auth (True RPC):** It calls the `sso-worker` via Cloudflare Service Bindings (`this.env.SSO_SERVICE.getMe(token)`) to strictly enforce RS256 JWT validation.
- **Miniflare Compatibility Bypass:** If auth fails, the proxy creates a mock `WebSocketPair`, upgrades the connection locally (returning `101 Switching Protocols`), and immediately closes it with `1008 Policy Violation`. This completely circumvents a known Miniflare/workerd local emulator crash triggered when returning non-101 responses during Upgrade requests over Service Bindings.
- **Durable Object Routing:** If auth succeeds, the user is mapped to a partition using `getPartitionId(userId)` (e.g. `partition-1`) and the request is proxied to the corresponding Durable Object stub.

### 2. The Stateful Hub (`realtime-hub.ts`)
The `RealtimeHub` is a Cloudflare Durable Object that manages stateful WebSocket connections.
- **Hibernation API:** Connects clients via `this.ctx.acceptWebSocket()` and immediately hibernates, ensuring the DO does not consume active compute time while connections are idle.
- **Attachments:** Uses `server.serializeAttachment()` to store essential metadata (e.g. `userId`, `connId`) directly on the socket instance, surviving DO evictions/hibernations.
- **Message Dispatching:** Broadcasts messages locally to all connected WebSockets within the partition, or forwards messages to the `realtime-events-queue` if cross-partition broadasting is required.
- **Heartbeats & Disconnects:** Natively catches `webSocketClose` and `webSocketError` events via the Hibernation API lifecycle hooks.

### 3. Asynchronous Events (`realtime-events-queue`)
Cloudflare Queues drive the internal server-to-server pub/sub network.
- **Event Ingestion:** Microservices (like `payment-worker` or `email-worker`) push events into the `realtime-events-queue`.
- **Batch Processing:** The `queue` handler in `index.ts` receives events in batches, deduplicates them, groups them by target partition, and makes parallel HTTP requests to the corresponding Durable Object partitions to broadcast the events to live clients.

## Development & Production Variances

### Realtime routing in local and deployed environments

The browser requests a short-lived single-use ticket from `/api/realtime-ticket`, then connects to `/api/realtime-stream` on the same origin. Pages routes the upgrade directly to `REALTIME_HUB`; it does not proxy the WebSocket response through a worker service binding. This also works when realtime runs as an auxiliary worker in `sso-worker`'s combined `dev:stack`, which has no standalone port 8790.

For an explicitly standalone local realtime server, set `VITE_REALTIME_WS_PORT` to its listening port before building the frontend. Without that override, the same-origin route is used. Both the default and named dev configurations declare `RealtimeHub` with `new_sqlite_classes`, and the owning worker binds its own class without `script_name`. External consumers continue using the worker's script name.

### SSO JWT Validation
The system relies on RS256 asymmetric signatures managed exclusively by the `sso-worker`. Old dependencies on symmetric keys (like `SUPABASE_JWT_SECRET` or `HS256` fallbacks) have been completely removed across the ecosystem, enforcing a strict zero-trust boundary.
