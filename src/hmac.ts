import { createHmac, timingSafeEqual } from 'node:crypto';

export function signBody(
  rawBody: string,
  secret: string,
  timestamp: string,
): string {
  return createHmac('sha256', secret)
    .update(`${timestamp}.${rawBody}`)
    .digest('hex');
}

export function verifyBody(input: {
  rawBody: string;
  secret: string;
  signature: string | undefined;
  timestamp: string | undefined;
  nowMs?: number;
  skewMs?: number;
}): boolean {
  const signature = (input.signature ?? '').trim().toLowerCase();
  const timestamp = (input.timestamp ?? '').trim();
  if (!input.secret || !/^[0-9a-f]+$/.test(signature) || !timestamp) return false;
  const timestampMs = Number.parseInt(timestamp, 10);
  const now = input.nowMs ?? Date.now();
  if (!Number.isFinite(timestampMs)) return false;
  if (Math.abs(now - timestampMs) > (input.skewMs ?? 5 * 60 * 1000)) return false;
  const expected = signBody(input.rawBody, input.secret, timestamp);
  const left = Buffer.from(expected);
  const right = Buffer.from(signature);
  if (left.length !== right.length) return false;
  return timingSafeEqual(left, right);
}
