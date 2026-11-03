import { getPartitionId, TOTAL_PARTITIONS } from './utils';

const TICKET_TTL = 30_000;
const RATE_WINDOW = 60_000;
const MAX_TICKETS_PER_WINDOW = 20;
interface Ticket {
  userId: string;
  expiresAt: number;
}
interface Budget {
  count: number;
  expiresAt: number;
}
export function ticketPartition(ticket: string): number | null {
  const match = /^(\d+)\.(\d{13})\.([a-f0-9]{64})$/.exec(ticket);
  const partition = match ? Number(match[1]) : -1;
  return partition >= 0 && partition < TOTAL_PARTITIONS ? partition : null;
}

/** Ephemeral authorization stored in the existing DO key/value storage.
 * No access token, signing secret, new SQL table or metadata column is needed. */
export class ConnectionTickets {
  constructor(private storage: DurableObjectStorage) {}
  async issue(userId: string): Promise<{ ticket: string; expiresAt: number }> {
    const now = Date.now(),
      expiresAt = now + TICKET_TTL;
    const nonce = [...crypto.getRandomValues(new Uint8Array(32))]
      .map((v) => v.toString(16).padStart(2, '0'))
      .join('');
    const budgetEnd = (Math.floor(now / RATE_WINDOW) + 1) * RATE_WINDOW;
    await this.storage.transaction(async (txn) => {
      const key = `ws-auth:${budgetEnd}:${userId}`;
      const budget = await txn.get<Budget>(key);
      if ((budget?.count || 0) >= MAX_TICKETS_PER_WINDOW)
        throw new Error('RATE_LIMITED');
      await txn.put(key, {
        count: (budget?.count || 0) + 1,
        expiresAt: budgetEnd,
      });
      await txn.put(`ws-ticket:${expiresAt}:${nonce}`, {
        userId,
        expiresAt,
      } satisfies Ticket);
    });
    const alarm = await this.storage.getAlarm();
    if (alarm === null || alarm > expiresAt)
      await this.storage.setAlarm(expiresAt);
    return {
      ticket: `${getPartitionId(userId)}.${expiresAt}.${nonce}`,
      expiresAt,
    };
  }
  async consume(ticket: string): Promise<string | null> {
    const partition = ticketPartition(ticket);
    if (partition === null) return null;
    const [, expiry, nonce] = ticket.split('.');
    if (Number(expiry) <= Date.now()) return null;
    return this.storage.transaction(async (txn) => {
      const key = `ws-ticket:${expiry}:${nonce}`;
      const record = await txn.get<Ticket>(key);
      if (
        !record ||
        record.expiresAt <= Date.now() ||
        getPartitionId(record.userId) !== partition
      )
        return null;
      await txn.delete(key);
      return record.userId;
    });
  }
  async cleanup(): Promise<void> {
    const now = Date.now();
    let next = Infinity;
    for (const prefix of ['ws-ticket:', 'ws-auth:']) {
      const records = await this.storage.list<Ticket | Budget>({
        prefix,
        limit: 256,
      });
      const expired: string[] = [];
      for (const [key, value] of records) {
        if (value.expiresAt <= now) expired.push(key);
        else {
          next = Math.min(next, value.expiresAt);
          break;
        }
      }
      if (expired.length) await this.storage.delete(expired);
      if (expired.length === 256) next = Math.min(next, now + 1000);
    }
    if (Number.isFinite(next)) await this.storage.setAlarm(next);
  }
}
