import { describe, it, expect } from 'vitest';
import { getPartitionId, TOTAL_PARTITIONS } from '../utils';

describe('getPartitionId', () => {
  it('returns consistent values for the same input', () => {
    const userId = 'user-abc-123';
    const result1 = getPartitionId(userId);
    const result2 = getPartitionId(userId);
    expect(result1).toBe(result2);
  });

  it('returns values in range 0 to TOTAL_PARTITIONS-1', () => {
    const testIds = [
      'user-1',
      'user-2',
      'a',
      'z',
      'very-long-user-id-that-goes-on-and-on-and-on',
      '550e8400-e29b-41d4-a716-446655440000',
      '',
    ];

    for (const id of testIds) {
      const result = getPartitionId(id);
      expect(result).toBeGreaterThanOrEqual(0);
      expect(result).toBeLessThan(TOTAL_PARTITIONS);
    }
  });

  it('returns different values for different inputs (with high probability)', () => {
    const results = new Set<number>();
    // Generate 100 unique user IDs — we expect at least 5 distinct partitions
    for (let i = 0; i < 100; i++) {
      results.add(getPartitionId(`user-${i}`));
    }
    expect(results.size).toBeGreaterThanOrEqual(5);
  });

  it('distributes evenly across partitions for sample user IDs', () => {
    const counts = new Array(TOTAL_PARTITIONS).fill(0);
    const numUsers = 1000;

    for (let i = 0; i < numUsers; i++) {
      const partition = getPartitionId(`user-uuid-${i}-${Math.random().toString(36)}`);
      counts[partition]++;
    }

    // Each partition should get roughly 10% (100 users) — allow 40% deviation (60-140 range)
    const expectedPerPartition = numUsers / TOTAL_PARTITIONS;
    for (let i = 0; i < TOTAL_PARTITIONS; i++) {
      expect(counts[i]).toBeGreaterThan(expectedPerPartition * 0.6);
      expect(counts[i]).toBeLessThan(expectedPerPartition * 1.4);
    }
  });

  it('handles empty string gracefully', () => {
    const result = getPartitionId('');
    expect(result).toBe(0);
  });

  it('handles special characters', () => {
    const ids = [
      'user@example.com',
      'user+special!chars',
      '日本語ユーザー',
      '🎉🎊',
    ];

    for (const id of ids) {
      const result = getPartitionId(id);
      expect(result).toBeGreaterThanOrEqual(0);
      expect(result).toBeLessThan(TOTAL_PARTITIONS);
    }
  });
});

describe('TOTAL_PARTITIONS', () => {
  it('is 10', () => {
    expect(TOTAL_PARTITIONS).toBe(10);
  });
});
