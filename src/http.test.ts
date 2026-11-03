import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { signBody } from './hmac.js';
import { startControlServer } from './http.js';
import { MemoryOutbox } from './outbox.js';
import { SparkWatcher, type SparkSocket } from './watcher.js';

class FakeSocket implements SparkSocket {
  readonly channels = new Map<string, (data: unknown) => void>();

  subscribe(channel: string, onData: (data: unknown) => void): void {
    this.channels.set(channel, onData);
  }

  unsubscribe(channel: string): void {
    this.channels.delete(channel);
  }
}

const SPARK =
  'spark1pgssyele0qrcjdheeq2a0zmpwdwvj3r4f4stkuju0fp36g6grapv2w7l8am2cp';
const OTHER =
  'spark1pgssxqwa7rqecec39m0c3p7vj2k3xqfkpcg030eyt6mnmqquvrrvglqtyg6q6k';

const SECRET = 'control-secret';

function watcher(): SparkWatcher {
  return new SparkWatcher({
    outbox: new MemoryOutbox(),
    socket: new FakeSocket(),
    network: 'MAINNET',
    now: () => 1,
    deliver: async () => 'ok',
  });
}

async function listen(target: SparkWatcher): Promise<{
  port: number;
  close: () => Promise<void>;
}> {
  const server = startControlServer({ port: 0, secret: SECRET, watcher: target });
  await new Promise<void>((resolve) => server.once('listening', () => resolve()));
  const address = server.address();
  const port = address && typeof address === 'object' ? address.port : 0;
  return {
    port,
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}

function signed(body: string): Record<string, string> {
  const timestamp = String(Date.now());
  return {
    'content-type': 'application/json',
    'x-sparkscan-timestamp': timestamp,
    'x-sparkscan-signature': signBody(body, SECRET, timestamp),
  };
}

describe('control server', () => {
  it('reports how many addresses are held', async () => {
    const held = watcher();
    held.subscribe(SPARK, 'wallet', '02aa');
    const server = await listen(held);
    try {
      const response = await fetch(`http://127.0.0.1:${server.port}/healthz`);
      assert.equal(response.status, 200);
      assert.deepEqual(await response.json(), { ok: true, held: 1 });
    } finally {
      await server.close();
    }
  });

  it('apply replaces the held wallets with the posted list', async () => {
    const held = watcher();
    held.subscribe(SPARK, 'wallet', '02aa');
    const server = await listen(held);
    try {
      const body = JSON.stringify({
        action: 'apply',
        addresses: [
          {
            sparkAddress: OTHER,
            kind: 'wallet',
            identityPubkey: '02bb',
          },
        ],
      });
      const response = await fetch(
        `http://127.0.0.1:${server.port}/control/subscriptions`,
        { method: 'POST', headers: signed(body), body },
      );
      assert.equal(response.status, 200);
      assert.deepEqual(held.heldAddresses(), [OTHER]);
    } finally {
      await server.close();
    }
  });
});
