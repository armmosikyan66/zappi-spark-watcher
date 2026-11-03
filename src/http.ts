import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { verifyBody } from './hmac.js';
import type { WatchedAddress } from './registry.js';
import type { SparkWatcher } from './watcher.js';

const MAX_BODY = 65_536;

export function startControlServer(input: {
  port: number;
  secret: string;
  watcher: SparkWatcher;
}): ReturnType<typeof createServer> {
  const server = createServer((request, response) => {
    void handle(request, response, input.secret, input.watcher);
  });
  server.on('error', (error) => {
    console.error(`[spark-watcher] control server: ${error.message}`);
    process.exit(1);
  });
  server.listen(input.port);
  return server;
}

async function handle(
  request: IncomingMessage,
  response: ServerResponse,
  secret: string,
  watcher: SparkWatcher,
): Promise<void> {
  if (request.method === 'GET' && request.url === '/healthz') {
    send(response, 200, {
      ok: true,
      held: watcher.heldAddresses().length,
    });
    return;
  }
  if (request.method !== 'POST' || request.url !== '/control/subscriptions') {
    send(response, 404, { ok: false });
    return;
  }
  const rawBody = await readBody(request);
  if (rawBody == null) {
    send(response, 413, { ok: false });
    return;
  }
  const ok = verifyBody({
    rawBody,
    secret,
    signature: header(request, 'x-sparkscan-signature'),
    timestamp: header(request, 'x-sparkscan-timestamp'),
  });
  if (!ok) {
    send(response, 401, { ok: false });
    return;
  }
  let body: unknown;
  try {
    body = JSON.parse(rawBody) as unknown;
  } catch {
    send(response, 400, { ok: false });
    return;
  }
  if (!body || typeof body !== 'object') {
    send(response, 400, { ok: false });
    return;
  }
  const record = body as Record<string, unknown>;
  if (record.action === 'apply') {
    const addresses = readAddresses(record.addresses);
    if (!addresses) {
      send(response, 400, { ok: false });
      return;
    }
    watcher.apply(addresses);
    send(response, 200, { ok: true });
    return;
  }
  const sparkAddress =
    typeof record.sparkAddress === 'string' ? record.sparkAddress.trim() : '';
  const kind = record.kind === 'pot' ? 'pot' : 'wallet';
  const identityPubkey =
    typeof record.identityPubkey === 'string' ? record.identityPubkey.trim() : '';
  if (!sparkAddress || (record.action !== 'subscribe' && record.action !== 'unsubscribe')) {
    send(response, 400, { ok: false });
    return;
  }
  if (record.action === 'subscribe') {
    watcher.subscribe(sparkAddress, kind, identityPubkey || undefined);
  } else watcher.unsubscribe(sparkAddress);
  send(response, 200, { ok: true });
}

function readAddresses(value: unknown): WatchedAddress[] | null {
  if (!Array.isArray(value)) return null;
  const addresses: WatchedAddress[] = [];
  for (const row of value) {
    if (!row || typeof row !== 'object') return null;
    const item = row as Record<string, unknown>;
    const sparkAddress =
      typeof item.sparkAddress === 'string' ? item.sparkAddress.trim() : '';
    if (!sparkAddress) return null;
    const identityPubkey =
      typeof item.identityPubkey === 'string' ? item.identityPubkey.trim() : '';
    addresses.push({
      sparkAddress,
      kind: item.kind === 'pot' ? 'pot' : 'wallet',
      ...(identityPubkey ? { identityPubkey } : {}),
    });
  }
  return addresses;
}

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}

function readBody(request: IncomingMessage): Promise<string | null> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let size = 0;
    request.on('data', (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY) {
        resolve(null);
        request.destroy();
        return;
      }
      chunks.push(chunk);
    });
    request.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    request.on('error', reject);
  });
}

function send(response: ServerResponse, status: number, body: unknown): void {
  if (response.writableEnded) return;
  response.writeHead(status, { 'content-type': 'application/json' });
  response.end(JSON.stringify(body));
}
