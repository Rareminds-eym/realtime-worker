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

### Miniflare Service Binding Limitation
Cloudflare Pages edge functions perfectly proxy WebSockets over Service Bindings (`env.REALTIME_WORKER.fetch()`) in production without any modifications. 
However, the local Miniflare emulator attempts to serialize WebSocket Upgrade Responses as JSON across workers, resulting in `Unexpected end of JSON input` or `Broken pipe` crashes. 

**Solution:** In the frontend's `wsRealtimeClient.ts`, if Vite runs in `import.meta.env.DEV` mode (on `localhost:8788`), it bypasses the Pages proxy entirely and connects directly to the underlying `realtime-worker` running on port `8790`. This ensures seamless local testing while preserving the robust Pages proxy architecture for production.

### SSO JWT Validation
The system relies on RS256 asymmetric signatures managed exclusively by the `sso-worker`. Old dependencies on symmetric keys (like `SUPABASE_JWT_SECRET` or `HS256` fallbacks) have been completely removed across the ecosystem, enforcing a strict zero-trust boundary.
