import { WorkerEntrypoint } from 'cloudflare:workers';
import { QueueMessageBody } from './realtime-hub';
export { RealtimeHub } from './realtime-hub';
export type { QueueMessageBody };

import { getPartitionId, TOTAL_PARTITIONS } from './utils';

export default class RealtimeWorker extends WorkerEntrypoint<Env> {
  declare env: Env;

  /**
   * WebSocket upgrade handler — used in local dev where the frontend
   * connects directly to ws://localhost:8790, bypassing Pages Functions.
   *
   * Extracts JWT from Sec-WebSocket-Protocol, verifies via SSO,
   * then proxies the upgrade to the DO stub (same partition logic
   * as the production direct DO binding path).
   *
   * In production, Pages Functions bind directly to REALTIME_HUB
   * and call the DO stub via the Durable Object binding.
   */
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Not Found', { status: 404 });
    }

    try {
      // Extract JWT from Sec-WebSocket-Protocol header
      const protocols = request.headers.get('Sec-WebSocket-Protocol') || '';
      const parts = protocols.split(',').map(p => p.trim());
      const tokenIndex = parts.indexOf('access_token');
      const token = tokenIndex !== -1 && tokenIndex < parts.length - 1 ? parts[tokenIndex + 1] : '';

      if (!token) {
        return new Response(
          JSON.stringify({ error: 'Missing auth token in Sec-WebSocket-Protocol' }),
          { status: 401, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Verify JWT via SSO
      if (!this.env.SSO_SERVICE) {
        console.error(JSON.stringify({ message: 'SSO_SERVICE binding not configured' }));
        return new Response(
          JSON.stringify({ error: 'Server configuration error' }),
          { status: 500, headers: { 'Content-Type': 'application/json' } }
        );
      }
      interface SsoService {
        getMe(token: string): Promise<{ sub?: string }>;
      }
      const ssoService = this.env.SSO_SERVICE as unknown as SsoService;
      let userId: string;
      try {
        const result = await ssoService.getMe(token);
        if (!result?.sub || typeof result.sub !== 'string') {
          throw new Error('Invalid sub claim from SSO');
        }
        userId = result.sub;
      } catch (err) {
        console.error(JSON.stringify({
          message: 'SSO verification failed',
          error: err instanceof Error ? err.message : String(err),
        }));
        return new Response(
          JSON.stringify({ error: 'Authentication failed' }),
          { status: 401, headers: { 'Content-Type': 'application/json' } }
        );
      }

      // Map userId → partition → DO stub
      const partitionId = getPartitionId(userId);
      const id = this.env.REALTIME_HUB.idFromName(`partition-${partitionId}`);
      const stub = this.env.REALTIME_HUB.get(id);

      // Forward the upgrade request to the DO (same headers)
      const doUrl = new URL('http://do/connect');
      const response = await stub.fetch(new Request(doUrl, {
        headers: request.headers,
      }));

      console.log(JSON.stringify({
        message: 'WebSocket upgrade successful',
        userId,
        partitionId,
        status: response.status,
      }));

      return response;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(JSON.stringify({
        message: 'WebSocket upgrade failed',
        error: message,
      }));
      return new Response(JSON.stringify({ error: 'Upgrade failed' }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * Queue Consumer — fans out realtime events to DO partitions.
   *
   * Two routing modes:
   * 1. Broadcast (target = 'broadcast'): sent to ALL partitions.
   * 2. Targeted (target = userId): sent ONLY to the partition that owns the user.
   *
   * CRITICAL: acks are deferred until AFTER dispatch succeeds.
   * Per Cloudflare Queues best practice, msg.ack() is only called
   * once the downstream DO dispatch is confirmed.
   */
  async queue(batch: MessageBatch<QueueMessageBody>): Promise<void> {
    // Group messages by target partition to minimize DO invocations
    const partitionBatches = new Map<number, QueueMessageBody[]>();

    for (const msg of batch.messages) {
      try {
        const body = msg.body;
        const target = body.target || 'broadcast';

        if (target === 'broadcast') {
          // Fan-out to all partitions, skip the source partition to prevent echo loops
          const sourcePartitionId = body.sourcePartitionId;
          for (let i = 0; i < TOTAL_PARTITIONS; i++) {
            if (i === sourcePartitionId) continue;
            if (!partitionBatches.has(i)) partitionBatches.set(i, []);
            partitionBatches.get(i)!.push(body.event ?? body);
          }
        } else {
          // Targeted to specific user's partition
          const partitionId = getPartitionId(target);
          if (!partitionBatches.has(partitionId)) partitionBatches.set(partitionId, []);
          partitionBatches.get(partitionId)!.push(body.event ?? body);
        }
      } catch (err) {
        console.error(JSON.stringify({
          message: 'Failed to group queue message',
          error: err instanceof Error ? err.message : String(err),
        }));
        msg.retry();
      }
    }

    if (partitionBatches.size === 0) {
      batch.ackAll();
      return;
    }

    // Dispatch batched messages to each DO partition
    const entries = Array.from(partitionBatches.entries());
    const results = await Promise.allSettled(
      entries.map(([partitionId, messages]) => {
        const id = this.env.REALTIME_HUB.idFromName(`partition-${partitionId}`);
        const stub = this.env.REALTIME_HUB.get(id);
        return stub.publishBatch(messages);
      })
    );

    // Check for failures — any failed partition means retry entire batch
    // (broadcast messages touch all partitions, so partial failure requires retry)
    const failures = results.filter(r => r.status === 'rejected');
    if (failures.length > 0) {
      for (const result of results) {
        if (result.status === 'rejected') {
          console.error(JSON.stringify({
            message: 'Partition dispatch failed',
            error: result.reason instanceof Error ? result.reason.message : String(result.reason),
          }));
        }
      }
      batch.retryAll();
    } else {
      batch.ackAll();
    }
  }
}
