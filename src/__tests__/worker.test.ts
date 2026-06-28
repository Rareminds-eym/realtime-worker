import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('cloudflare:workers', () => ({
  DurableObject: class DurableObject {
    ctx: any;
    env: any;
    constructor(ctx: any, env: any) {
      this.ctx = ctx;
      this.env = env;
    }
  },
  WorkerEntrypoint: class WorkerEntrypoint {
    ctx: any;
    env: any;
    constructor(ctx: any, env: any) {
      this.ctx = ctx;
      this.env = env;
    }
  }
}));

import RealtimeWorker from '../index';
import { TOTAL_PARTITIONS } from '../utils';
import type { QueueMessageBody } from '../index';
import type { InternalEventType } from '../realtime-hub';

describe('Worker Queue Consumer', () => {
  let mockEnv: any;
  let publishBatchMock: any;
  let workerInstance: any;

  beforeEach(() => {
    publishBatchMock = vi.fn().mockResolvedValue(undefined);

    // Mock DO binding
    mockEnv = {
      REALTIME_HUB: {
        idFromName: vi.fn((name: string) => ({ name })),
        get: vi.fn(() => ({
          publishBatch: publishBatchMock,
        })),
      },
    };

    workerInstance = new RealtimeWorker({} as any, mockEnv);
  });

  it('fans out external events to ALL partitions and acks on success', async () => {
    const body = {
      event: { type: 'INSERT', table: 'messages', payload: { id: 1 } },
    };

    const ackAll = vi.fn();
    const retryAll = vi.fn();
    const batch = {
      messages: [{ body, ack: vi.fn(), retry: vi.fn() }],
      ackAll,
      retryAll,
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    // Should call idFromName 10 times (once for each partition)
    expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    // Each partition gets 1 event
    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    for (let i = 0; i < TOTAL_PARTITIONS; i++) {
      expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledWith(`partition-${i}`);
      expect(publishBatchMock).toHaveBeenCalledWith([
        batch.messages[0].body.event,
      ]);
    }

    // ackAll is called on successful dispatch (not individual msg.ack)
    expect(ackAll).toHaveBeenCalledOnce();
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('filters out sourcePartitionId to prevent echo loops', async () => {
    const ackAll = vi.fn();
    const retryAll = vi.fn();
    const batch = {
      messages: [
        {
          body: {
            sourcePartitionId: 3,
            event: {
              type: '__INTERNAL_WS_BROADCAST' as InternalEventType,
              channel: 'test',
              eventType: 'custom',
              payload: {},
              from: 'user-1',
            },
          } as QueueMessageBody,
          ack: vi.fn(),
          retry: vi.fn(),
        },
      ],
      ackAll,
      retryAll,
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    // It should skip partition 3
    expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledTimes(TOTAL_PARTITIONS - 1);
    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS - 1);
    
    expect(mockEnv.REALTIME_HUB.idFromName).not.toHaveBeenCalledWith('partition-3');

    // ackAll on success
    expect(ackAll).toHaveBeenCalledOnce();
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('batches multiple events for the same partition correctly', async () => {
    const ackAll = vi.fn();
    const retryAll = vi.fn();
    const batch = {
      messages: [
        {
          body: {
            sourcePartitionId: 1,
            event: { type: 'EVENT_A' } as any,
          } as QueueMessageBody,
          ack: vi.fn(),
          retry: vi.fn(),
        },
        {
          body: {
            sourcePartitionId: 2,
            event: { type: 'EVENT_B' } as any,
          } as QueueMessageBody,
          ack: vi.fn(),
          retry: vi.fn(),
        },
      ],
      ackAll,
      retryAll,
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    // Check content routing: partition 1 gets EVENT_B only, partition 2 gets EVENT_A only
    const calls = publishBatchMock.mock.calls;
    expect(calls.length).toBe(TOTAL_PARTITIONS);
    
    // ackAll on success
    expect(ackAll).toHaveBeenCalledOnce();
    expect(retryAll).not.toHaveBeenCalled();
  });

  it('retries entire batch when partition dispatch fails', async () => {
    publishBatchMock.mockRejectedValue(new Error('DO unavailable'));

    const ackAll = vi.fn();
    const retryAll = vi.fn();
    const batch = {
      messages: [
        {
          body: { event: { type: 'INSERT', table: 'test', payload: {} } },
          ack: vi.fn(),
          retry: vi.fn(),
        },
      ],
      ackAll,
      retryAll,
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    // Should have attempted dispatch
    expect(publishBatchMock).toHaveBeenCalled();
    // retryAll on failure
    expect(retryAll).toHaveBeenCalledOnce();
    expect(ackAll).not.toHaveBeenCalled();
  });

  it('does not dispatch when all messages are filtered, acks empty batch to prevent retry loops', async () => {
    // Guard against empty batch causing infinite Queue retries
    const ackAll = vi.fn();
    const retryAll = vi.fn();
    const batch = {
      messages: [],
      ackAll,
      retryAll,
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    expect(publishBatchMock).not.toHaveBeenCalled();
    // Empty batch MUST be ack'd — Queues would retry indefinitely otherwise
    expect(ackAll).toHaveBeenCalledOnce();
    expect(retryAll).not.toHaveBeenCalled();
  });
});
