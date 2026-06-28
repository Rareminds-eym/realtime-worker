/**
 * RealtimeHub — Hibernation-Safe Durable Object
 *
 * Manages WebSocket connections, subscription matching, broadcast channels,
 * and presence state for a single partition of the hash ring.
 *
 * CRITICAL DESIGN DECISION — Hibernation Safety:
 *   Cloudflare's WebSocket Hibernation API evicts the DO from memory between
 *   messages to save costs. When it wakes, the constructor re-runs and ALL
 *   in-memory state (Maps, Sets, variables) is EMPTY.
 *
 *   Therefore:
 *   - Subscriptions are stored in SQLite (survive hibernation).
 *   - Presence state is stored in SQLite (survive hibernation).
 *   - Per-WebSocket metadata is stored via serializeAttachment (survive hibernation).
 *   - The only in-memory state is `partitionId`, which is restored from
 *     WebSocket attachments on every handler invocation.
 *
 * @see https://developers.cloudflare.com/durable-objects/api/websockets/
 */
import { DurableObject } from 'cloudflare:workers';
import { getPartitionId } from './utils';

// ─── Types ────────────────────────────────────────────────────────────────────

/** Metadata attached to each WebSocket via serializeAttachment (survives hibernation). */
interface WsAttachment {
  wsId: string;
  userId: string;
  partitionId: number;
}

/** Presence information for a user in a channel. */
interface PresenceInfo {
  userId: string;
  userName: string;
  userType: string;
  status: string;
  lastSeen: string;
  conversationId?: string;
}

/** Internal event types for cross-partition communication via Queue. */
export type InternalEventType =
  | '__INTERNAL_WS_BROADCAST'
  | '__INTERNAL_WS_PRESENCE_JOIN'
  | '__INTERNAL_WS_PRESENCE_LEAVE'
  | '__INTERNAL_WS_PRESENCE_HEARTBEAT';

/** The structure of messages sent through the Cloudflare Queue. */
export interface QueueMessageBody {
  /** The target partition or broadcast flag. Defaults to broadcast if omitted. */
  target?: string | 'broadcast';
  /** The event payload. */
  event?: any;
  [key: string]: any;
}

// ─── Durable Object ───────────────────────────────────────────────────────────

export class RealtimeHub extends DurableObject<Env> {
  /**
   * Partition ID for this DO instance. Set on first fetch and restored
   * from WebSocket attachments on subsequent handler invocations after
   * hibernation wake-up.
   */
  private partitionId: number = -1;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);

    // Initialize SQLite schema — runs once, idempotent (Rule 7.4.5)
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS subscriptions (
          ws_id TEXT NOT NULL,
          table_name TEXT NOT NULL,
          event TEXT NOT NULL DEFAULT '*',
          filter TEXT,
          PRIMARY KEY (ws_id, table_name, event, filter)
        )
      `);

      // Presence MUST be in SQLite, NOT in-memory Map.
      // In-memory state is destroyed on Hibernation API wake-up.
      this.ctx.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS presence (
          channel TEXT NOT NULL,
          user_id TEXT NOT NULL,
          user_name TEXT NOT NULL DEFAULT '',
          user_type TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'online',
          last_seen TEXT NOT NULL,
          conversation_id TEXT,
          PRIMARY KEY (channel, user_id)
        )
      `);
    });
  }

  // ─── RPC Method (called by Queue Consumer via Worker) ─────────────────────

  /**
   * Receives a batch of events from the Queue Consumer and processes them.
   * This is the cross-partition communication channel.
   *
   * @param events - Array of realtime events to process
   */
  async publishBatch(events: Record<string, unknown>[]): Promise<void> {
    for (const event of events) {
      const eventType = typeof event.type === 'string' ? event.type : '';

      switch (eventType) {
        case '__INTERNAL_WS_BROADCAST': {
          const channel = typeof event.channel === 'string' ? event.channel : '';
          const eventTypeStr = typeof event.eventType === 'string' ? event.eventType : '';
          const from = typeof event.from === 'string' ? event.from : '';
          if (!channel) break;
          this.broadcastChannel(channel, eventTypeStr, event.payload, from);
          break;
        }

        case '__INTERNAL_WS_PRESENCE_JOIN': {
          const channel = typeof event.channel === 'string' ? event.channel : '';
          if (!channel) break;
          this.sqlJoinPresence(channel, event.info as PresenceInfo);
          this.syncPresence(channel);
          break;
        }

        case '__INTERNAL_WS_PRESENCE_LEAVE': {
          const channel = typeof event.channel === 'string' ? event.channel : '';
          const userId = typeof event.userId === 'string' ? event.userId : '';
          if (!channel || !userId) break;
          this.sqlLeavePresence(channel, userId);
          this.syncPresence(channel);
          break;
        }

        case '__INTERNAL_WS_PRESENCE_HEARTBEAT': {
          const channel = typeof event.channel === 'string' ? event.channel : '';
          const userId = typeof event.userId === 'string' ? event.userId : '';
          if (!channel || !userId) break;
          this.sqlUpdateHeartbeat(channel, userId, event.status as string | undefined);
          this.syncPresence(channel);
          break;
        }

        default:
          // Database change events from notifyRealtime()
          this.broadcastToSubscribers(event);
      }
    }
  }

  // ─── RPC Methods ──────────────────────────────────────────────────────────
  
  async getStats(): Promise<Record<string, unknown>> {
    let connections = 0;
    try {
      connections = this.ctx.getWebSockets().length;
    } catch {}

    let presences = 0;
    try {
      const cursor = this.ctx.storage.sql.exec('SELECT COUNT(*) as count FROM presence');
      const res = [...cursor].pop();
      presences = (res?.count as number) || 0;
    } catch {}

    return {
      partitionId: this.partitionId,
      connections,
      presences
    };
  }

  // ─── WebSocket Lifecycle ──────────────────────────────────────────────────

  /**
   * Handles incoming HTTP requests. Only WebSocket upgrades are accepted.
   * All other requests return 404.
   */
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Not found', { status: 404 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Extract JWT from protocols
    const protocols = request.headers.get('Sec-WebSocket-Protocol') || '';
    const parts = protocols.split(',').map(p => p.trim());
    const tokenIndex = parts.indexOf('access_token');
    const token = tokenIndex !== -1 && tokenIndex < parts.length - 1 ? parts[tokenIndex + 1] : '';

    let userId = 'unknown';
    try {
      if (token) {
        // The token is already verified by the proxy in index.ts.
        // We just need to decode the payload to extract the userId.
        const payloadBase64 = token.split('.')[1];
        if (payloadBase64) {
          // Fix base64 padding and decode
          const base64 = payloadBase64.replace(/-/g, '+').replace(/_/g, '/');
          // Restore padding stripped by base64url (RFC 4648 §5)
          const padded = base64.padEnd(base64.length + (4 - base64.length % 4) % 4, '=');
          const jsonPayload = decodeURIComponent(
            atob(padded)
              .split('')
              .map(c => '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2))
              .join('')
          );
          const payload = JSON.parse(jsonPayload);
          if (payload.sub) {
            userId = payload.sub;
          }
        }
      }
    } catch (e) {
      console.warn(JSON.stringify({
        message: '[RealtimeHub] JWT decode failed inside DO',
        error: String(e),
      }));
    }
    this.partitionId = getPartitionId(userId);

    if (userId === 'unknown') {
      server.close(1008, 'Unauthorized');
      return new Response(null, {
        status: 101,
        webSocket: client,
      });
    }

    const wsId = crypto.randomUUID();

    console.log(JSON.stringify({
      message: 'WebSocket accepted by DO',
      userId,
      partitionId: this.partitionId,
      wsId,
    }));

    // Enable Hibernation API — DO can be evicted between messages
    this.ctx.acceptWebSocket(server);

    // Attach metadata — survives hibernation via serializeAttachment
    const attachment: WsAttachment = { wsId, userId, partitionId: this.partitionId };
    server.serializeAttachment(attachment);

    // Send connection confirmation to client
    server.send(JSON.stringify({
      type: 'connected',
      connId: wsId,
    }));

    // Respond with the selected subprotocol. The browser requires a
    // Sec-WebSocket-Protocol response header matching one of the requested
    // protocols, otherwise it may close the connection.
    // Conditionally set per RFC 6455 §4.2.2: only echo if the client offered one.
    const requestedProtocols = request.headers.get('Sec-WebSocket-Protocol');
    const responseHeaders: Record<string, string> = {};
    if (requestedProtocols) {
      responseHeaders['Sec-WebSocket-Protocol'] = 'access_token';
    }

    return new Response(null, {
      status: 101,
      webSocket: client,
      headers: responseHeaders,
    });
  }

  /**
   * Handles incoming WebSocket messages from clients.
   * Called by Hibernation API — DO may have been evicted and re-constructed
   * since the last message, so all state must be read from SQLite/attachments.
   */
  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): void {
    if (typeof message !== 'string') return; // Binary messages not supported

    let data: Record<string, unknown>;
    try {
      data = JSON.parse(message);
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
      return;
    }

    const raw = ws.deserializeAttachment();
    if (!raw) {
      console.warn(JSON.stringify({ message: 'WebSocket has no attachment, skipping message' }));
      return;
    }
    const attachment = raw as WsAttachment;

    // Restore partitionId from attachment (may have been lost during hibernation)
    this.partitionId = attachment.partitionId;

    switch (data.action) {
      case 'subscribe':
        this.handleSubscribe(attachment.wsId, data);
        break;

      case 'send-broadcast':
        this.handleSendBroadcast(attachment.userId, data);
        break;

      case 'join-presence':
        this.handleJoinPresence(attachment.userId, data);
        break;

      case 'heartbeat':
        this.handleHeartbeat(attachment.userId, data);
        break;

      case 'leave-presence':
        this.handleLeavePresence(attachment.userId, data);
        break;

      default:
        ws.send(JSON.stringify({
          type: 'error',
          message: `Unknown action: ${data.action}`,
        }));
    }
  }

  /**
   * Handles WebSocket close events.
   * Cleans up subscriptions and presence for the disconnected user.
   */
  webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): void {
    const raw = ws.deserializeAttachment();
    if (!raw) {
      console.warn(JSON.stringify({ message: 'WebSocket has no attachment on close' }));
      return;
    }
    const attachment = raw as WsAttachment;
    this.partitionId = attachment.partitionId;

    // Clean up all subscriptions for this WebSocket
    this.ctx.storage.sql.exec(
      `DELETE FROM subscriptions WHERE ws_id = ?`,
      attachment.wsId
    );

    // Clean up presence from ALL channels this user was in
    const channels = this.ctx.storage.sql.exec(
      `SELECT DISTINCT channel FROM presence WHERE user_id = ?`,
      attachment.userId
    ).toArray();

    for (const row of channels) {
      const channel = row.channel as string;
      this.sqlLeavePresence(channel, attachment.userId);
      this.syncPresence(channel);

      // Notify other partitions about the leave
      this.env.REALTIME_EVENTS_QUEUE.send({
        sourcePartitionId: this.partitionId,
        event: {
          type: '__INTERNAL_WS_PRESENCE_LEAVE' as InternalEventType,
          channel,
          userId: attachment.userId,
        },
      }).catch((err: unknown) => {
        console.error(JSON.stringify({
          message: 'Queue send failed (close cleanup)',
          error: String(err),
        }));
      });
    }
  }

  /**
   * Handles WebSocket errors. Delegates to close handler for cleanup.
   */
  webSocketError(_ws: WebSocket, error: unknown): void {
    console.error(JSON.stringify({
      message: 'WebSocket error',
      error: String(error),
    }));
    // webSocketClose is called by the Hibernation API runtime — no need to delegate
    // Duplicate cleanup of subscriptions/presence would cause undefined behavior.
  }

  // ─── Action Handlers ──────────────────────────────────────────────────────

  /**
   * Registers a subscription for a table/event/filter combination.
   * Client will receive matching events via the WebSocket.
   */
  private handleSubscribe(wsId: string, data: Record<string, unknown>): void {
    this.ctx.storage.sql.exec(
      `INSERT OR IGNORE INTO subscriptions (ws_id, table_name, event, filter)
       VALUES (?, ?, ?, ?)`,
      wsId,
      data.table as string,
      (data.event as string) || '*',
      (data.filter as string) || null
    );
  }

  /**
   * Handles a broadcast message from a client.
   * 1. Delivers to local subscribers immediately (zero latency).
   * 2. Queues for fan-out to other 9 partitions via sourcePartitionId.
   */
  private handleSendBroadcast(userId: string, data: Record<string, unknown>): void {
    // Local delivery — instant
    this.broadcastChannel(
      data.channel as string,
      data.eventType as string,
      data.payload,
      userId
    );

    // Cross-partition delivery via Queue (skips this partition)
    this.env.REALTIME_EVENTS_QUEUE.send({
      sourcePartitionId: this.partitionId,
      event: {
        type: '__INTERNAL_WS_BROADCAST' as InternalEventType,
        channel: data.channel,
        eventType: data.eventType,
        payload: data.payload,
        from: userId,
      },
    }).catch((err: unknown) => {
      console.error(JSON.stringify({
        message: 'Queue send failed (broadcast)',
        error: String(err),
      }));
    });
  }

  /**
   * Handles a presence join from a client.
   * 1. Persists to SQLite locally.
   * 2. Syncs presence to local subscribers.
   * 3. Queues for fan-out to other 9 partitions.
   */
  private handleJoinPresence(userId: string, data: Record<string, unknown>): void {
    const info: PresenceInfo = {
      userId,
      userName: (data.userName as string) || '',
      userType: (data.userType as string) || '',
      status: (data.status as string) || 'online',
      lastSeen: new Date().toISOString(),
      conversationId: data.conversationId as string | undefined,
    };

    this.sqlJoinPresence(data.channel as string, info);
    this.syncPresence(data.channel as string);

    this.env.REALTIME_EVENTS_QUEUE.send({
      sourcePartitionId: this.partitionId,
      event: {
        type: '__INTERNAL_WS_PRESENCE_JOIN' as InternalEventType,
        channel: data.channel,
        info,
      },
    }).catch((err: unknown) => {
      console.error(JSON.stringify({
        message: 'Queue send failed (presence join)',
        error: String(err),
      }));
    });
  }

  /**
   * Handles a heartbeat from a client — updates last_seen in SQLite.
   */
  private handleHeartbeat(userId: string, data: Record<string, unknown>): void {
    const channel = data.channel as string;
    this.sqlUpdateHeartbeat(
      channel,
      userId,
      data.status as string | undefined
    );
    this.syncPresence(channel);

    this.env.REALTIME_EVENTS_QUEUE.send({
      sourcePartitionId: this.partitionId,
      event: {
        type: '__INTERNAL_WS_PRESENCE_HEARTBEAT' as InternalEventType,
        channel,
        userId,
        status: data.status,
      },
    }).catch((err: unknown) => {
      console.error(JSON.stringify({
        message: 'Queue send failed (heartbeat)',
        error: String(err),
      }));
    });
  }

  /**
   * Handles a presence leave from a client.
   */
  private handleLeavePresence(userId: string, data: Record<string, unknown>): void {
    this.sqlLeavePresence(data.channel as string, userId);
    this.syncPresence(data.channel as string);

    this.env.REALTIME_EVENTS_QUEUE.send({
      sourcePartitionId: this.partitionId,
      event: {
        type: '__INTERNAL_WS_PRESENCE_LEAVE' as InternalEventType,
        channel: data.channel,
        userId,
      },
    }).catch((err: unknown) => {
      console.error(JSON.stringify({
        message: 'Queue send failed (presence leave)',
        error: String(err),
      }));
    });
  }

  // ─── SQLite Presence (Hibernation-Safe) ───────────────────────────────────

  /**
   * Inserts or updates a user's presence in a channel.
   * Uses INSERT OR REPLACE for idempotent upsert.
   */
  private sqlJoinPresence(channel: string, info: PresenceInfo): void {
    this.ctx.storage.sql.exec(
      `INSERT OR REPLACE INTO presence
       (channel, user_id, user_name, user_type, status, last_seen, conversation_id)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      channel,
      info.userId,
      info.userName,
      info.userType,
      info.status,
      info.lastSeen,
      info.conversationId || null
    );
  }

  /** Removes a user from a presence channel. */
  private sqlLeavePresence(channel: string, userId: string): void {
    this.ctx.storage.sql.exec(
      `DELETE FROM presence WHERE channel = ? AND user_id = ?`,
      channel,
      userId
    );
  }

  /** Updates the last_seen timestamp (and optionally status) for a heartbeat. */
  private sqlUpdateHeartbeat(channel: string, userId: string, status?: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE presence SET last_seen = ?, status = COALESCE(?, status)
       WHERE channel = ? AND user_id = ?`,
      new Date().toISOString(),
      status || null,
      channel,
      userId
    );
  }

  // ─── Presence Sync ────────────────────────────────────────────────────────

  /**
   * Prunes stale presence entries (>120s without heartbeat),
   * reads active users from SQLite, and broadcasts presence_sync
   * to all subscribers of this channel.
   */
  private syncPresence(channelName: string): void {
    const STALE_TIMEOUT_MS = 120_000; // 2 minutes
    const cutoff = new Date(Date.now() - STALE_TIMEOUT_MS).toISOString();

    // Prune stale entries
    this.ctx.storage.sql.exec(
      `DELETE FROM presence WHERE channel = ? AND last_seen < ?`,
      channelName,
      cutoff
    );

    // Read active entries
    const rows = this.ctx.storage.sql.exec(
      `SELECT user_id, user_name, user_type, status, last_seen, conversation_id
       FROM presence WHERE channel = ?`,
      channelName
    ).toArray();

    const users: PresenceInfo[] = rows.map((r) => ({
      userId: r.user_id as string,
      userName: r.user_name as string,
      userType: r.user_type as string,
      status: r.status as string,
      lastSeen: r.last_seen as string,
      conversationId: (r.conversation_id as string) || undefined,
    }));

    const payload = JSON.stringify({
      type: 'presence_sync',
      channel: channelName,
      users,
      timestamp: new Date().toISOString(),
    });

    this.sendToSubscribers(`__presence:${channelName}`, '*', null, payload);
  }

  // ─── Broadcasters ─────────────────────────────────────────────────────────

  /**
   * Broadcasts a channel event to all subscribers of `__broadcast:{channel}`.
   */
  private broadcastChannel(
    channel: string,
    eventType: string,
    payload: unknown,
    from: string
  ): void {
    const data = JSON.stringify({
      type: 'broadcast',
      channel,
      eventType,
      payload,
      from,
      timestamp: new Date().toISOString(),
    });

    this.sendToSubscribers(`__broadcast:${channel}`, '*', null, data);
  }

  /**
   * Broadcasts a database change event to subscribers of the affected table.
   */
  private broadcastToSubscribers(event: Record<string, unknown>): void {
    const data = JSON.stringify({
      type: 'change',
      table: event.table,
      event: event.type,
      payload: event.payload,
      timestamp: new Date().toISOString(),
    });

    this.sendToSubscribers(
      event.table as string,
      event.type as string,
      event.payload as Record<string, unknown> | null,
      data
    );
  }

  // ─── Subscription Matcher ─────────────────────────────────────────────────

  /**
   * Queries SQLite for matching subscriptions and sends the JSON payload
   * to each matched WebSocket.
   *
   * Filter format: "column=value" (e.g., "conversationId=abc-123")
   * Filters are evaluated against the event payload.
   *
   * @param targetTable - Table name or internal channel (e.g., "__broadcast:chat")
   * @param targetType - Event type (e.g., "INSERT", "*")
   * @param payload - Event payload for filter matching (null if no filter needed)
   * @param jsonString - Pre-serialized JSON string to send
   */
  private sendToSubscribers(
    targetTable: string,
    targetType: string,
    payload: Record<string, unknown> | null,
    jsonString: string
  ): void {
    const cursor = this.ctx.storage.sql.exec(
      `SELECT ws_id, filter FROM subscriptions
       WHERE table_name = ? AND (event = ? OR event = '*')`,
      targetTable,
      targetType
    );

    // Build a map of wsId → WebSocket from active hibernated connections
    const activeSockets = this.ctx.getWebSockets();
    const wsMap = new Map<string, WebSocket>();

    for (const ws of activeSockets) {
      const raw = ws.deserializeAttachment();
      if (!raw) continue;
      wsMap.set((raw as WsAttachment).wsId, ws);
    }

    for (const row of cursor) {
      const filter = row.filter as string | null;

      // Apply client-side filter if present
      if (filter && payload) {
        const eqIdx = filter.indexOf('=');
        if (eqIdx > 0) {
          const col = filter.substring(0, eqIdx);
          const raw = filter.substring(eqIdx + 1);
          // Handle filters like "collegeId=eq.abc" → extract "abc"
          const cleanVal = raw.includes('.') ? raw.split('.')[1] : raw;
          // URL query params arrive as strings, payload values may be numbers — coerce comparison is intentional.
          if (col && cleanVal && String(payload[col]) != cleanVal) continue;
        }
      }

      const ws = wsMap.get(row.ws_id as string);
      if (ws) {
        try {
          ws.send(jsonString);
        } catch {
          // WebSocket may have been closed between query and send — safe to ignore
        }
      }
    }
  }
}
