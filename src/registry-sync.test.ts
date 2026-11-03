import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { syncRegistryUntilConnected } from './registry-sync.js';

describe('syncRegistryUntilConnected', () => {
  it('retries a refused registry and stops after the first success', async () => {
    const sleeps: number[] = [];
    const logs: string[] = [];
    const applied: string[][] = [];
    let calls = 0;
    await syncRegistryUntilConnected({
      url: 'http://nest/registry',
      secret: 'secret',
      target: {
        apply: (rows) => applied.push(rows.map((row) => row.sparkAddress)),
        watchedLine: () => 'watched count=1 spark1a',
      },
      log: (line) => logs.push(line),
      sleep: async (ms) => {
        sleeps.push(ms);
      },
      fetchImpl: (async () => {
        calls += 1;
        if (calls < 3) {
          throw Object.assign(new Error('fetch failed'), {
            cause: { code: 'ECONNREFUSED', address: '127.0.0.1' },
          });
        }
        return new Response(
          JSON.stringify({
            addresses: [{ sparkAddress: 'spark1a', kind: 'wallet' }],
          }),
          { status: 200 },
        );
      }) as typeof fetch,
    });

    assert.equal(calls, 3);
    assert.deepEqual(sleeps, [2_000, 4_000]);
    assert.match(logs[0] ?? '', /registry retry attempt=1 fetch failed \(ECONNREFUSED 127.0.0.1\)/);
    assert.match(logs[1] ?? '', /registry retry attempt=2/);
    assert.equal(
      logs[2],
      'registry addresses=1 watched count=1 spark1a',
    );
    assert.deepEqual(applied, [['spark1a']]);
  });

  it('treats a non-2xx registry response as a retry', async () => {
    const sleeps: number[] = [];
    let calls = 0;
    await syncRegistryUntilConnected({
      url: 'http://nest/registry',
      secret: 'secret',
      target: {
        apply: () => undefined,
        watchedLine: () => 'watched count=0',
      },
      log: () => undefined,
      sleep: async (ms) => {
        sleeps.push(ms);
      },
      fetchImpl: (async () => {
        calls += 1;
        if (calls === 1) return new Response('no', { status: 503 });
        return new Response(JSON.stringify({ addresses: [] }), { status: 200 });
      }) as typeof fetch,
    });
    assert.equal(calls, 2);
    assert.deepEqual(sleeps, [2_000]);
  });
});
