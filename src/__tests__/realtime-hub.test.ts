import { describe, it, expect, vi, beforeEach } from 'vitest';

vi.mock('cloudflare:workers', () => ({
  DurableObject: class DurableObject {
    ctx: any;
    env: any;
    constructor(ctx: any, env: any) {
      this.ctx = ctx;
      this.env = env;
    }
  }
}));

import { RealtimeHub } from '../realtime-hub';

describe('RealtimeHub', () => {
  let mockEnv: any;
  let mockState: any;
  let execMock: any;
  let prepareMock: any;
  let queueSendMock: any;
  let websockets: any[] = [];
  let hub: RealtimeHub;

  beforeEach(() => {
    execMock = vi.fn().mockImplementation((_sql: string) => {
      // Return a mock cursor that is both iterable and has toArray()
      const arr: any[] = [];
      (arr as any).toArray = () => [];
      return arr;
    });
    prepareMock = vi.fn().mockReturnValue({
      all: vi.fn().mockReturnValue([]),
      run: vi.fn(),
      raw: vi.fn().mockReturnValue([]),
    });

    queueSendMock = vi.fn().mockResolvedValue(undefined);
    websockets = [];

    mockState = {
      blockConcurrencyWhile: vi.fn((cb) => cb()),
      storage: {
        sql: {
          exec: execMock,
          prepare: prepareMock,
        },
      },
      getWebSockets: vi.fn(() => websockets),
      acceptWebSocket: vi.fn((ws, _tags) => {
        websockets.push(ws);
      }),
    };

    mockEnv = {
      REALTIME_EVENTS_QUEUE: {
        send: queueSendMock,
      },
    };

    hub = new RealtimeHub(mockState as any, mockEnv as any);
  });

  it('initializes SQLite tables on instantiation', () => {
    expect(mockState.blockConcurrencyWhile).toHaveBeenCalled();
    expect(execMock).toHaveBeenCalledTimes(2);
    const sql1 = execMock.mock.calls[0][0];
    const sql2 = execMock.mock.calls[1][0];
    expect(sql1).toContain('CREATE TABLE IF NOT EXISTS subscriptions');
    expect(sql2).toContain('CREATE TABLE IF NOT EXISTS presence');
  });

  it('can publish batch events and filter subscriptions', async () => {
    // Mock SQLite to return 1 ws_id for the subscription query
    execMock.mockImplementation((sql: string) => {
      if (sql.includes('SELECT ws_id, filter FROM subscriptions')) {
        const arr = [{ ws_id: 'ws-1', filter: null }];
        (arr as any).toArray = () => arr;
        return arr;
      }
      return Object.assign([], { toArray: () => [] });
    });

    const mockWs = {
      send: vi.fn(),
      deserializeAttachment: vi.fn().mockReturnValue({ wsId: 'ws-1' }),
    };
    websockets.push(mockWs);

    const event = {
      table: 'messages',
      type: 'INSERT',
      payload: { id: 1, text: 'hello' },
    };

    await hub.publishBatch([event as any]);

    // Should have called exec for checking subscriptions
    expect(execMock).toHaveBeenCalledWith(
      expect.stringContaining('SELECT ws_id, filter FROM subscriptions'),
      'messages',
      'INSERT'
    );

    // Should have sent the event to the websocket
    expect(mockWs.send).toHaveBeenCalled();
    const sent = JSON.parse(mockWs.send.mock.calls[0][0]);
    expect(sent.type).toBe('change');
    expect(sent.table).toBe('messages');
    expect(sent.event).toBe('INSERT');
    expect(sent.payload).toEqual({ id: 1, text: 'hello' });
    expect(sent.timestamp).toBeTypeOf('string');
  });

  it('can process heartbeat events and sync presence', async () => {
    // Mock the heartbeat call inside publishBatch
    const event = {
      type: '__INTERNAL_WS_PRESENCE_HEARTBEAT',
      channel: 'test-channel',
      userId: 'user-1',
      status: 'away',
    };

    execMock.mockImplementation((sql: string) => {
      if (sql.includes('SELECT user_id')) {
        // syncPresence query
        const arr = [{
          user_id: 'user-1',
          user_name: 'Test User',
          user_type: 'learner',
          status: 'away',
          last_seen: '2023-01-01T00:00:00Z',
          conversation_id: null
        }];
        (arr as any).toArray = () => arr;
        return arr;
      }
      if (sql.includes('SELECT ws_id, filter FROM subscriptions')) {
        const arr = [{ ws_id: 'ws-1', filter: null }];
        (arr as any).toArray = () => arr;
        return arr;
      }
      return Object.assign([], { toArray: () => [] });
    });

    const mockWs = {
      send: vi.fn(),
      deserializeAttachment: vi.fn().mockReturnValue({ wsId: 'ws-1' }),
    };
    websockets.push(mockWs);

    // Run the batch
    await hub.publishBatch([event as any]);

    // sqlUpdateHeartbeat was called
    expect(execMock).toHaveBeenCalledWith(
      expect.stringContaining('UPDATE presence SET last_seen'),
      expect.any(String),
      'away',
      'test-channel',
      'user-1'
    );
    
    // syncPresence was called and broadcasted to ws-1
    expect(mockWs.send).toHaveBeenCalled();
    const sent = JSON.parse(mockWs.send.mock.calls[0][0]);
    expect(sent.type).toBe('presence_sync');
    expect(sent.channel).toBe('test-channel');
    expect(sent.users[0].status).toBe('away');
  });

  it('prunes inactive presence entries on publishBatch', async () => {
    execMock.mockImplementation(() => {
      return Object.assign([], { toArray: () => [] });
    });
    
    await hub.publishBatch([{
      type: '__INTERNAL_WS_PRESENCE_HEARTBEAT',
      channel: 'test-channel',
      userId: 'user-1'
    }] as any);

    expect(execMock).toHaveBeenCalledWith(
      expect.stringContaining('DELETE FROM presence'),
      'test-channel',
      expect.anything()
    );
  });
});
