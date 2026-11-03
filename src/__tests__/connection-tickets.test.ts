import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { ConnectionTickets, ticketPartition } from '../connection-tickets';
import { getPartitionId } from '../utils';
let records: Map<string, { expiresAt: number }>;
let store: DurableObjectStorage;
beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1800000000000);
  records = new Map();
  let alarm: number | null = null;
  let serial = Promise.resolve();
  const mock = {
    get: async (key: string) => records.get(key),
    put: async (key: string, value: { expiresAt: number }) => {
      records.set(key, value);
    },
    delete: async (keys: string | string[]) => {
      for (const key of Array.isArray(keys) ? keys : [keys])
        records.delete(key);
    },
    getAlarm: async () => alarm,
    setAlarm: async (value: number) => {
      alarm = value;
    },
    list: async ({ prefix, limit }: { prefix: string; limit: number }) =>
      new Map(
        [...records.entries()]
          .filter(([key]) => key.startsWith(prefix))
          .sort(([a], [b]) => a.localeCompare(b))
          .slice(0, limit),
      ),
    transaction: (fn: (txn: unknown) => Promise<unknown>) => {
      const task = serial.then(() => fn(mock));
      serial = task.then(
        () => {},
        () => {},
      );
      return task;
    },
  };
  store = mock as unknown as DurableObjectStorage;
});
afterEach(() => vi.useRealTimers());
it('binds tickets to the authenticated user and consumes each ticket only once', async () => {
  const tickets = new ConnectionTickets(store);
  const result = await tickets.issue('user-a');
  expect(ticketPartition(result.ticket)).toBe(getPartitionId('user-a'));
  expect(await tickets.consume(result.ticket)).toBe('user-a');
  expect(await tickets.consume(result.ticket)).toBeNull();
});
it('allows only one simultaneous redemption', async () => {
  const tickets = new ConnectionTickets(store);
  const { ticket } = await tickets.issue('user-a');
  expect(
    (
      await Promise.all([tickets.consume(ticket), tickets.consume(ticket)])
    ).filter(Boolean),
  ).toEqual(['user-a']);
});
it('rejects expiry and cleans unused tickets without new SQL tables', async () => {
  const tickets = new ConnectionTickets(store);
  const { ticket } = await tickets.issue('user-a');
  vi.advanceTimersByTime(30001);
  expect(await tickets.consume(ticket)).toBeNull();
  await tickets.cleanup();
  expect([...records.keys()].some((k) => k.startsWith('ws-ticket:'))).toBe(
    false,
  );
  vi.advanceTimersByTime(60000);
  await tickets.cleanup();
  expect(records.size).toBe(0);
});
it('rejects a forged partition without consuming the authentic ticket', async () => {
  const tickets = new ConnectionTickets(store);
  const { ticket } = await tickets.issue('user-a');
  const pieces = ticket.split('.');
  pieces[0] = String((Number(pieces[0]) + 1) % 10);
  expect(await tickets.consume(pieces.join('.'))).toBeNull();
  expect(await tickets.consume(ticket)).toBe('user-a');
});
it('permits concurrent tabs but bounds ticket issuance per user', async () => {
  const tickets = new ConnectionTickets(store);
  const tokens = await Promise.all(
    Array.from({ length: 20 }, () => tickets.issue('user-a')),
  );
  expect(new Set(tokens.map((t) => t.ticket)).size).toBe(20);
  await expect(tickets.issue('user-a')).rejects.toThrow('RATE_LIMITED');
  expect(await tickets.issue('user-b')).toBeTruthy();
});
it.each([
  '',
  '../user',
  '0.expired.token',
  '999.1800000000000.' + 'a'.repeat(64),
])('rejects malformed tickets: %s', async (ticket) => {
  expect(await new ConnectionTickets(store).consume(ticket)).toBeNull();
});
