import { Centrifuge, type Subscription } from 'centrifuge';
import WebSocket from 'ws';
import type { SparkSocket } from './watcher.js';

export class CentrifugeSocket implements SparkSocket {
  private readonly client: Centrifuge;
  private readonly subs = new Map<string, Subscription>();
  private subscriptionError: (channel: string, error: unknown) => void = () =>
    undefined;

  constructor(url: string) {
    this.client = new Centrifuge(url, { websocket: WebSocket });
  }

  /** A per-address subscribe that Sparkscan refused. Silent failures look like no payments. */
  onSubscriptionError(
    listener: (channel: string, error: unknown) => void,
  ): void {
    this.subscriptionError = listener;
  }

  connect(): void {
    this.client.connect();
  }

  onConnect(listener: () => void): void {
    this.client.on('connected', listener);
  }

  onDisconnect(listener: () => void): void {
    this.client.on('disconnected', listener);
  }

  subscribe(channel: string, onData: (data: unknown) => void): void {
    if (this.subs.has(channel)) return;
    const sub = this.client.newSubscription(channel);
    sub.on('publication', (ctx) => {
      onData(ctx.data);
    });
    sub.on('error', (ctx) => {
      this.subscriptionError(channel, ctx);
    });
    sub.subscribe();
    this.subs.set(channel, sub);
  }

  unsubscribe(channel: string): void {
    const sub = this.subs.get(channel);
    if (!sub) return;
    sub.unsubscribe();
    this.client.removeSubscription(sub);
    this.subs.delete(channel);
  }
}
