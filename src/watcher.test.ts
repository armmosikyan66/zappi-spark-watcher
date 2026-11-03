import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { MemoryOutbox } from './outbox.js';
import {
  diffSubscriptions,
  inChannelFor,
  outChannelFor,
} from './registry.js';
import { SparkWatcher, type SparkSocket } from './watcher.js';

const SPARK =
  'spark1pgssyele0qrcjdheeq2a0zmpwdwvj3r4f4stkuju0fp36g6grapv2w7l8am2cp';
const OTHER =
  'spark1pgssxqwa7rqecec39m0c3p7vj2k3xqfkpcg030eyt6mnmqquvrrvglqtyg6q6k';

class FakeSocket implements SparkSocket {
  readonly channels = new Map<string, (data: unknown) => void>();

  subscribe(channel: string, onData: (data: unknown) => void): void {
    this.channels.set(channel, onData);
  }

  unsubscribe(channel: string): void {
    this.channels.delete(channel);
  }

  publish(channel: string, data: unknown): void {
    this.channels.get(channel)?.(data);
  }
}

function channelsOf(address: string): [string, string] {
  const incoming = inChannelFor('MAINNET', address);
  const outgoing = outChannelFor('MAINNET', address);
  assert.ok(incoming);
  assert.ok(outgoing);
  return [incoming, outgoing];
}

describe('diffSubscriptions', () => {
  it('unsubscribes anything missing from the desired set, pots included', () => {
    const current = new Map([
      ['pot1', { sparkAddress: 'pot1', kind: 'pot' as const }],
      ['wallet1', { sparkAddress: 'wallet1', kind: 'wallet' as const }],
    ]);
    const diff = diffSubscriptions(current, []);
    assert.deepEqual(diff.unsubscribe, ['pot1', 'wallet1']);
    assert.deepEqual(diff.subscribe, []);
  });

  it('keeps a pot the desired set still lists', () => {
    const current = new Map([
      ['pot1', { sparkAddress: 'pot1', kind: 'pot' as const }],
    ]);
    const diff = diffSubscriptions(current, [
      { sparkAddress: 'pot1', kind: 'pot' },
    ]);
    assert.deepEqual(diff.unsubscribe, []);
  });
});

describe('SparkWatcher', () => {
  it('appends a publication before delivery and drops it after 2xx', async () => {
    const outbox = new MemoryOutbox();
    const socket = new FakeSocket();
    const delivered: string[] = [];
    const watcher = new SparkWatcher({
      outbox,
      socket,
      network: 'MAINNET',
      now: () => 1_000,
      deliver: async (rawBody) => {
        delivered.push(rawBody);
        return 'ok';
      },
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    const [incoming] = channelsOf(SPARK);
    socket.publish(incoming, { id: 'tx-1' });
    assert.equal(outbox.size(), 1);
    await watcher.flush();
    assert.equal(outbox.size(), 0);
    assert.equal(delivered.length, 1);
    const body = JSON.parse(delivered[0] ?? '{}') as { payload: { id: string } };
    assert.equal(body.payload.id, 'tx-1');
  });

  it('keeps the row when Nest is down and deletes it on the retry', async () => {
    const outbox = new MemoryOutbox();
    const socket = new FakeSocket();
    let now = 1_000;
    let calls = 0;
    const watcher = new SparkWatcher({
      outbox,
      socket,
      network: 'MAINNET',
      now: () => now,
      deliver: async () => {
        calls += 1;
        return calls === 1 ? 'retry' : 'ok';
      },
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    socket.publish(channelsOf(SPARK)[0], { id: 'tx-2' });
    await watcher.flush();
    assert.equal(outbox.size(), 1);
    now += 5_000;
    await watcher.flush();
    assert.equal(outbox.size(), 0);
  });

  it('emits a gap window for addresses held across a disconnect', () => {
    const outbox = new MemoryOutbox();
    let now = 5_000;
    const watcher = new SparkWatcher({
      outbox,
      socket: new FakeSocket(),
      network: 'MAINNET',
      now: () => now,
      deliver: async () => 'ok',
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    watcher.onDisconnect();
    now = 9_000;
    watcher.onConnect();
    const row = outbox.due(now, 1)[0];
    assert.ok(row);
    const body = JSON.parse(row.body) as { kind: string; addresses: string[] };
    assert.equal(body.kind, 'gap');
    assert.deepEqual(body.addresses, [SPARK]);
  });

  it('subscribes both directions and removes both on unsubscribe', () => {
    const socket = new FakeSocket();
    const watcher = new SparkWatcher({
      outbox: new MemoryOutbox(),
      socket,
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
    });
    const [incoming, outgoing] = channelsOf(SPARK);
    watcher.subscribe(SPARK, 'pot');
    assert.equal(socket.channels.has(incoming), true);
    assert.equal(socket.channels.has(outgoing), true);
    watcher.unsubscribe(SPARK);
    assert.deepEqual(watcher.heldAddresses(), []);
    assert.equal(socket.channels.has(incoming), false);
    assert.equal(socket.channels.has(outgoing), false);
  });

  it('lists the watched addresses with their kind', () => {
    const watcher = new SparkWatcher({
      outbox: new MemoryOutbox(),
      socket: new FakeSocket(),
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
    });
    assert.equal(watcher.watchedLine(), 'watched count=0');
    watcher.subscribe(SPARK, 'wallet');
    watcher.subscribe(OTHER, 'pot');
    assert.equal(
      watcher.watchedLine(),
      `watched count=2 ${SPARK} ${OTHER} (pot)`,
    );
  });

  it('turns an incoming or outgoing publication into a hint for the spark1 address', async () => {
    const outbox = new MemoryOutbox();
    const socket = new FakeSocket();
    const watcher = new SparkWatcher({
      outbox,
      socket,
      network: 'MAINNET',
      now: () => 1_000,
      deliver: async () => 'ok',
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    const [incoming, outgoing] = channelsOf(SPARK);
    socket.publish(incoming, { id: 'tx-in', type: 'spark_transfer', status: 'confirmed' });
    socket.publish(outgoing, { id: 'tx-out' });
    assert.equal(outbox.size(), 2);
    const first = JSON.parse(outbox.due(1_000, 2)[0]?.body ?? '{}') as {
      sparkAddress: string;
      channel: string;
    };
    assert.equal(first.sparkAddress, SPARK);
    assert.equal(first.channel, incoming);
    await watcher.flush();
    assert.equal(outbox.size(), 0);
  });

  it('ignores a publication on a channel it does not hold', () => {
    const outbox = new MemoryOutbox();
    const socket = new FakeSocket();
    const watcher = new SparkWatcher({
      outbox,
      socket,
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    socket.publish(channelsOf(OTHER)[0], { id: 'tx-other' });
    assert.equal(outbox.size(), 0);
  });

  it('skips an address that cannot become an sp1 identifier', () => {
    const socket = new FakeSocket();
    const logs: string[] = [];
    const watcher = new SparkWatcher({
      outbox: new MemoryOutbox(),
      socket,
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
      log: (line) => logs.push(line),
    });
    watcher.apply([{ sparkAddress: 'spark1bad', kind: 'wallet' }]);
    assert.deepEqual(watcher.heldAddresses(), []);
    assert.equal(socket.channels.size, 0);
    assert.ok(logs.some((line) => line.includes('undecodable')));
  });

  it('counts per-address publications', () => {
    const socket = new FakeSocket();
    const watcher = new SparkWatcher({
      outbox: new MemoryOutbox(),
      socket,
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    const [incoming] = channelsOf(SPARK);
    socket.publish(incoming, { id: 'tx-1' });
    socket.publish(incoming, { id: 'tx-2' });
    assert.equal(watcher.statsLine(), 'pubs per-address=2 hints=2 held=1');
  });

  it('keeps the held row when a later subscribe omits the pubkey', () => {
    const watcher = new SparkWatcher({
      outbox: new MemoryOutbox(),
      socket: new FakeSocket(),
      network: 'MAINNET',
      now: () => 1,
      deliver: async () => 'ok',
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet', identityPubkey: '02aa0' }]);
    watcher.subscribe(SPARK, 'wallet');
    assert.deepEqual(watcher.heldAddresses(), [SPARK]);
  });

  it('notifies on append and reports publication latency', () => {
    const outbox = new MemoryOutbox();
    const socket = new FakeSocket();
    const appended: number[] = [];
    const watcher = new SparkWatcher({
      outbox,
      socket,
      network: 'MAINNET',
      now: () => 10_000,
      deliver: async () => 'ok',
      onAppend: () => appended.push(outbox.size()),
    });
    watcher.apply([{ sparkAddress: SPARK, kind: 'wallet' }]);
    socket.publish(channelsOf(SPARK)[0], {
      id: 'tx-live',
      createdAt: '1970-01-01T00:00:00.000Z',
    });
    assert.deepEqual(appended, [1]);
    assert.equal(
      watcher.statsLine(),
      'pubs per-address=1 hints=1 held=1 latency p50=10000ms max=10000ms',
    );
  });
});
