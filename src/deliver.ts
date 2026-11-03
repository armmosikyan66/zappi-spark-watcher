import { Agent, fetch as undiciFetch } from 'undici';
import { signBody } from './hmac.js';

export type DeliveryResult = 'ok' | 'retry' | 'drop';

const TERMINAL = new Set([400, 401, 413]);

/** Reuse the TCP connection to Nest. A fresh handshake per hint costs more than the hint. */
const nestAgent = new Agent({
  keepAliveTimeout: 60_000,
  keepAliveMaxTimeout: 600_000,
  connections: 4,
});

export async function deliverHint(input: {
  url: string;
  secret: string;
  rawBody: string;
  fetchImpl?: typeof fetch;
  nowMs?: number;
}): Promise<DeliveryResult> {
  const timestamp = String(input.nowMs ?? Date.now());
  const signature = signBody(input.rawBody, input.secret, timestamp);
  try {
    const response = input.fetchImpl
      ? await input.fetchImpl(input.url, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-sparkscan-signature': signature,
            'x-sparkscan-timestamp': timestamp,
          },
          body: input.rawBody,
          signal: AbortSignal.timeout(8_000),
        })
      : await undiciFetch(input.url, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-sparkscan-signature': signature,
            'x-sparkscan-timestamp': timestamp,
          },
          body: input.rawBody,
          signal: AbortSignal.timeout(8_000),
          dispatcher: nestAgent,
        });
    if (response.ok) return 'ok';
    if (TERMINAL.has(response.status)) return 'drop';
    return 'retry';
  } catch {
    return 'retry';
  }
}

export function retryDelayMs(attempts: number): number {
  const step = Math.min(attempts, 6);
  return Math.min(60_000, 1_000 * 2 ** step);
}
