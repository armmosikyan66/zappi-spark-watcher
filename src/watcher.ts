import type { Outbox } from './outbox.js';
import { retryDelayMs, type DeliveryResult } from './deliver.js';
import {
  diffSubscriptions,
  inChannelFor,
  outChannelFor,
  type WatchedAddress,
} from './registry.js';

export interface SparkSocket {
  subscribe(channel: string, onData: (data: unknown) => void): void;
  unsubscribe(channel: string): void;
}

const LATENCY_CAP = 512;

export type SparkWatcherOptions = {
  outbox: Outbox;
  socket: SparkSocket;
  network: string;
  now: () => number;
  deliver: (rawBody: string) => Promise<DeliveryResult>;
  log?: (line: string) => void;
  /** Fired after a hint is durable in the outbox. Delivery should start from this. */
  onAppend?: () => void;
};

/**
 * Ingest only. One identity is two Sparkscan channels: incoming and outgoing,
 * named with the sp1 identifier. Publications land in the outbox before
 * delivery. A reconnect records a gap window so Nest can backfill the
 * addresses that were live. The hint envelope keeps the spark1 address so
 * Nest can resolve the user.
 */
export class SparkWatcher {
  private readonly held = new Map<string, WatchedAddress>();
  private readonly latencies: number[] = [];
  private stats = { perAddress: 0, hints: 0 };
  private disconnectedAt: number | null = null;
  private readonly log: (line: string) => void;

  constructor(private readonly options: SparkWatcherOptions) {
    this.log = options.log ?? (() => undefined);
  }

  /** Source counters for the periodic stats line. */
  statsLine(): string {
    const { perAddress, hints } = this.stats;
    const base = `pubs per-address=${perAddress} hints=${hints} held=${this.held.size}`;
    if (this.latencies.length === 0) return base;
    const sorted = [...this.latencies].sort((left, right) => left - right);
    const p50 = sorted[Math.floor((sorted.length - 1) / 2)] ?? 0;
    const max = sorted[sorted.length - 1] ?? 0;
    this.latencies.length = 0;
    return `${base} latency p50=${p50}ms max=${max}ms`;
  }

  heldAddresses(): string[] {
    return [...this.held.keys()];
  }

  /** One line listing everything the watcher currently holds. */
  watchedLine(): string {
    const rows = [...this.held.values()];
    if (rows.length === 0) return 'watched count=0';
    const list = rows
      .map((row) =>
        row.kind === 'pot' ? `${row.sparkAddress} (pot)` : row.sparkAddress,
      )
      .join(' ');
    return `watched count=${rows.length} ${list}`;
  }

  onDisconnect(): void {
    if (this.disconnectedAt != null) return;
    this.disconnectedAt = this.options.now();
  }

  onConnect(): void {
    const since = this.disconnectedAt;
    this.disconnectedAt = null;
    if (since == null || this.held.size === 0) return;
    const body = JSON.stringify({
      kind: 'gap',
      since: new Date(since).toISOString(),
      addresses: [...this.held.keys()],
    });
    this.options.outbox.append(body, this.options.now());
    this.options.onAppend?.();
    this.log(`gap window addresses=${this.held.size}`);
  }

  apply(next: readonly WatchedAddress[]): void {
    const diff = diffSubscriptions(this.held, next);
    for (const row of diff.subscribe) {
      if (!this.subscribeChannels(row.sparkAddress)) {
        this.log(`watch skip ${row.sparkAddress} undecodable`);
        continue;
      }
      this.held.set(row.sparkAddress, this.mergeRow(row));
      this.log(`watch +${row.sparkAddress} kind=${row.kind}`);
    }
    for (const sparkAddress of diff.unsubscribe) {
      this.unsubscribeChannels(sparkAddress);
      this.held.delete(sparkAddress);
      this.log(`watch -${sparkAddress}`);
    }
  }

  subscribe(
    sparkAddress: string,
    kind: WatchedAddress['kind'],
    identityPubkey?: string,
  ): void {
    const pubkey = identityPubkey?.trim().toLowerCase();
    this.apply([
      {
        sparkAddress,
        kind,
        ...(pubkey ? { identityPubkey: pubkey } : {}),
      },
      ...this.snapshot(),
    ]);
  }

  unsubscribe(sparkAddress: string): void {
    if (!this.held.has(sparkAddress)) return;
    this.unsubscribeChannels(sparkAddress);
    this.held.delete(sparkAddress);
    this.log(`watch -${sparkAddress}`);
  }

  async flush(limit = 10): Promise<void> {
    const now = this.options.now();
    let delivered = 0;
    for (const row of this.options.outbox.due(now, limit)) {
      const result = await this.options.deliver(row.body);
      if (result === 'retry') {
        this.options.outbox.fail(row.id, now, retryDelayMs(row.attempts));
        this.log(`hint retry id=${row.id} attempts=${row.attempts}`);
        continue;
      }
      this.options.outbox.succeed(row.id);
      if (result === 'ok') {
        delivered += 1;
      } else {
        this.log(`hint dropped id=${row.id}`);
      }
    }
    if (delivered > 0) this.log(`hints delivered=${delivered}`);
  }

  /** A registry row can omit the pubkey; keep the one already held. */
  private mergeRow(row: WatchedAddress): WatchedAddress {
    const existing = this.held.get(row.sparkAddress);
    if (!existing) return row;
    return {
      ...existing,
      ...row,
      identityPubkey: row.identityPubkey ?? existing.identityPubkey,
    };
  }

  /** Both directions. False when the address cannot become an sp1 identifier. */
  private subscribeChannels(sparkAddress: string): boolean {
    const incoming = inChannelFor(this.options.network, sparkAddress);
    const outgoing = outChannelFor(this.options.network, sparkAddress);
    if (!incoming || !outgoing) return false;
    for (const channel of [incoming, outgoing]) {
      this.options.socket.subscribe(channel, (data) => {
        this.onPublication(sparkAddress, channel, data);
      });
    }
    return true;
  }

  private unsubscribeChannels(sparkAddress: string): void {
    for (const channel of [
      inChannelFor(this.options.network, sparkAddress),
      outChannelFor(this.options.network, sparkAddress),
    ]) {
      if (channel) this.options.socket.unsubscribe(channel);
    }
  }

  private onPublication(
    sparkAddress: string,
    channel: string,
    data: unknown,
  ): void {
    const payload = parsePublication(data);
    if (!payload) return;
    this.stats.perAddress += 1;
    this.appendOutbox(sparkAddress, channel, payload);
  }

  private appendOutbox(
    sparkAddress: string,
    channel: string,
    payload: unknown,
  ): void {
    this.stats.hints += 1;
    const receivedAt = this.options.now();
    this.noteLatency(payload, receivedAt);
    if (payload && typeof payload === 'object') {
      this.log(
        `tx addr=${sparkAddress} ${describePublication(
          payload as Record<string, unknown>,
        )}`,
      );
    }
    const body = JSON.stringify({
      kind: 'transaction',
      sparkAddress,
      channel,
      receivedAt: new Date(receivedAt).toISOString(),
      payload,
    });
    this.options.outbox.append(body, receivedAt);
    this.options.onAppend?.();
  }

  private noteLatency(payload: unknown, receivedAt: number): void {
    const createdAt = readCreatedAt(payload);
    if (createdAt == null) return;
    const latency = receivedAt - createdAt;
    if (!Number.isFinite(latency)) return;
    this.latencies.push(Math.round(latency));
    if (this.latencies.length > LATENCY_CAP) {
      this.latencies.splice(0, this.latencies.length - LATENCY_CAP);
    }
  }

  private snapshot(): WatchedAddress[] {
    return [...this.held.values()];
  }
}

/** Centrifuge JSON publications arrive as an encoded string. */
function parsePublication(data: unknown): Record<string, unknown> | null {
  if (typeof data === 'string') {
    try {
      const parsed: unknown = JSON.parse(data);
      return parsed && typeof parsed === 'object'
        ? (parsed as Record<string, unknown>)
        : null;
    } catch {
      return null;
    }
  }
  return data && typeof data === 'object'
    ? (data as Record<string, unknown>)
    : null;
}

function readCreatedAt(payload: unknown): number | null {
  if (!payload || typeof payload !== 'object') return null;
  const record = payload as Record<string, unknown>;
  return (
    readTime(record['createdAt']) ??
    readTime(record['created_at']) ??
    readTime(record['timestamp'])
  );
}

function readTime(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value < 1e12 ? value * 1000 : value;
  }
  if (typeof value !== 'string' || !value.trim()) return null;
  if (/^\d+$/.test(value.trim())) {
    const parsed = Number(value);
    return parsed < 1e12 ? parsed * 1000 : parsed;
  }
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

/** One terminal line per matched transaction. */
function describePublication(payload: Record<string, unknown>): string {
  const parts: string[] = [];
  const push = (key: string, label: string): void => {
    const value = payload[key];
    if (value != null) parts.push(`${label}=${String(value)}`);
  };
  push('id', 'id');
  push('type', 'type');
  push('status', 'status');
  push('amount_sats', 'sats');
  push('amountSats', 'sats');
  push('token_amount', 'token');
  push('tokenAmount', 'token');
  push('token_address', 'tokenAddr');
  push('tokenAddress', 'tokenAddr');
  return parts.length > 0 ? parts.join(' ') : 'unknown-payload';
}
