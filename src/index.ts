import { WorkerEntrypoint } from 'cloudflare:workers';
export { RealtimeHub } from './realtime-hub';
import { QueueMessageBody } from './realtime-hub';
export type { QueueMessageBody };

// We export a helper to map user UUIDs to a DO partition
import { getPartitionId } from './utils';

export default class RealtimeWorker extends WorkerEntrypoint<Env> {
  declare env: Env;

  /**
   * HTTP fetch handler for proxying WebSocket upgrades.
   * Receives upgraded request directly from Pages API Gateway, verifies auth,
   * attaches partitionId, and routes to the correct Durable Object partition.
   */
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Not Found', { status: 404 });
    }

    try {
      // Extract JWT from Sec-WebSocket-Protocol
      const protocols = request.headers.get('Sec-WebSocket-Protocol') || '';
      const parts = protocols.split(',').map(p => p.trim());
      const tokenIndex = parts.indexOf('access_token');
      const token = tokenIndex !== -1 && tokenIndex < parts.length - 1 ? parts[tokenIndex + 1] : '';

      console.log(`[Realtime Worker] Extracted token length: ${token.length}, Token prefix: ${token.substring(0, 15)}...`);

      if (!token) {
        console.error('[Realtime Worker] No token provided in Sec-WebSocket-Protocol header');
        const pair = new WebSocketPair();
        (pair[1] as any).accept();
        pair[1].close(1008, 'Unauthorized: Expected auth token');
        return new Response(null, { status: 101, webSocket: pair[0], headers: { 'Sec-WebSocket-Protocol': 'access_token' } });
      }

      // Verify JWT using the SSO service
      let userId: string;
      try {
        const result = await this.env.SSO_SERVICE.getMe(token);
        userId = result.sub as string;
      } catch (err) {
        console.error(`[Realtime Worker] SSO verification failed:`, err);
        const pair = new WebSocketPair();
        (pair[1] as any).accept();
        pair[1].close(1008, 'Unauthorized: SSO verification failed');
        return new Response(null, { status: 101, webSocket: pair[0], headers: { 'Sec-WebSocket-Protocol': 'access_token' } });
      }

      // Route to correct partition
      const partitionId = getPartitionId(userId);
      const id = this.env.REALTIME_HUB.idFromName(`partition-${partitionId}`);
      const stub = this.env.REALTIME_HUB.get(id);

      // Pass the unmodified request to the DO to preserve the WebSocket socket
      try {
        return await stub.fetch(request);
      } catch (err) {
        console.error(`[Realtime Worker] DO proxy failed:`, err);
        const message = err instanceof Error ? err.message : String(err);
        return new Response(`Durable Object proxy failed: ${message}`, { status: 500 });
      }
    } catch (err) {
      console.error(`[Realtime Worker] Unexpected internal error:`, err);
      const message = err instanceof Error ? err.message : String(err);
      return new Response(`Unexpected internal error: ${message}`, { status: 500 });
    }
  }

  /**
   * Queue Consumer — fans out realtime events to DO partitions.
   *
   * Two routing modes:
   * 1. Broadcast (target = 'broadcast'): sent to ALL partitions.
   * 2. Targeted (target = userId): sent ONLY to the partition that owns the user.
   */
  async queue(batch: MessageBatch<QueueMessageBody>): Promise<void> {
    // Group messages by target partition to minimize DO invocations
    const partitionBatches = new Map<number, QueueMessageBody[]>();

    for (const msg of batch.messages) {
      try {
        const body = msg.body;
        const target = body.target || 'broadcast';

        if (target === 'broadcast') {
          // Fan-out to all 10 partitions, skip the source partition to prevent echo loops
          const sourcePartitionId = body.sourcePartitionId;
          for (let i = 0; i < 10; i++) {
            if (i === sourcePartitionId) continue;
            if (!partitionBatches.has(i)) partitionBatches.set(i, []);
            partitionBatches.get(i)!.push(body);
          }
        } else {
          // Targeted to specific user's partition
          const partitionId = getPartitionId(target);
          if (!partitionBatches.has(partitionId)) partitionBatches.set(partitionId, []);
          partitionBatches.get(partitionId)!.push(body);
        }
        msg.ack();
      } catch (err) {
        console.error(`[Realtime Worker] Failed to process message:`, err);
        msg.retry();
      }
    }

    // Dispatch batched messages to each DO partition
    const promises = Array.from(partitionBatches.entries()).map(([partitionId, messages]) => {
      const id = this.env.REALTIME_HUB.idFromName(`partition-${partitionId}`);
      const stub = this.env.REALTIME_HUB.get(id);
      return stub.publishBatch(messages);
    });

    await Promise.all(promises);
  }
}
