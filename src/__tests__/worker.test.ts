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

  it('fans out external events to ALL partitions', async () => {
    const body = {
      event: { type: 'INSERT', table: 'messages', payload: { id: 1 } },
    };

    const batch = {
      messages: [{ body, ack: vi.fn(), retry: vi.fn() }],
      ackAll: vi.fn(),
      retryAll: vi.fn(),
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    // Should call idFromName 10 times (once for each partition)
    expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    // Each partition gets 1 event
    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    for (let i = 0; i < TOTAL_PARTITIONS; i++) {
      expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledWith(`partition-${i}`);
      expect(publishBatchMock).toHaveBeenCalledWith([{
        event: batch.messages[0].body.event
      }]);
    }
  });

  it('filters out sourcePartitionId to prevent echo loops', async () => {
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
      ackAll: vi.fn(),
      retryAll: vi.fn(),
    } as unknown as MessageBatch<QueueMessageBody>;

    await workerInstance.queue(batch as any, mockEnv as Env);

    // It should skip partition 3
    expect(mockEnv.REALTIME_HUB.idFromName).toHaveBeenCalledTimes(TOTAL_PARTITIONS - 1);
    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS - 1);
    
    expect(mockEnv.REALTIME_HUB.idFromName).not.toHaveBeenCalledWith('partition-3');
  });

  it('batches multiple events for the same partition correctly', async () => {
    const batch = {
      messages: [
        {
          body: {
            sourcePartitionId: 1, // Skip 1
            event: { type: 'EVENT_A' } as any,
          } as QueueMessageBody,
          ack: vi.fn(),
          retry: vi.fn(),
        },
        {
          body: {
            sourcePartitionId: 2, // Skip 2
            event: { type: 'EVENT_B' } as any,
          } as QueueMessageBody,
          ack: vi.fn(),
          retry: vi.fn(),
        },
      ],
    };

    await workerInstance.queue(batch as any, mockEnv as Env);

    // Partition 0 gets both A and B
    // Partition 1 gets only B
    // Partition 2 gets only A
    // Partition 3 gets both A and B
    
    expect(publishBatchMock).toHaveBeenCalledTimes(TOTAL_PARTITIONS);
    
    const calls = publishBatchMock.mock.calls;
    expect(calls.length).toBe(TOTAL_PARTITIONS);
    
    // Collect (partitionName, events[]) pairs (order is insertion-order from Map)
    const nameCalls = mockEnv.REALTIME_HUB.idFromName.mock.calls.map((c: any) => c[0]);
    
    // Partition 0 must be called and get both A and B
    const p0Idx = nameCalls.indexOf('partition-0');
    expect(calls[p0Idx][0].length).toBe(2);
    expect(calls[p0Idx][0][0].event.type).toBe('EVENT_A');
    expect(calls[p0Idx][0][1].event.type).toBe('EVENT_B');
    
    // Partition 1 must get only B
    const p1Idx = nameCalls.indexOf('partition-1');
    expect(calls[p1Idx][0].length).toBe(1);
    expect(calls[p1Idx][0][0].event.type).toBe('EVENT_B');
    
    // Partition 2 must get only A
    const p2Idx = nameCalls.indexOf('partition-2');
    expect(calls[p2Idx][0].length).toBe(1);
    expect(calls[p2Idx][0][0].event.type).toBe('EVENT_A');
    
    // Check that msg.ack was called for both
    expect(batch.messages[0].ack).toHaveBeenCalled();
    expect(batch.messages[1].ack).toHaveBeenCalled();
  });
});
