import 'dotenv/config';
import { setDefaultResultOrder } from 'node:dns';
import { deliverHint } from './deliver.js';
import { startDeliveryLoop } from './delivery-loop.js';
import { startControlServer } from './http.js';
import { SqliteOutbox } from './sqlite-outbox.js';
import { CentrifugeSocket } from './centrifuge-socket.js';
import { SparkWatcher } from './watcher.js';
import { syncRegistryUntilConnected } from './registry-sync.js';

// Nest listens on IPv4. macOS resolves localhost to ::1 first, and that
// connection is refused before fetch falls back.
setDefaultResultOrder('ipv4first');

const network = process.env.SPARKSCAN_NETWORK?.trim() || 'MAINNET';
const wsUrl =
  process.env.SPARKSCAN_WS_URL?.trim() ||
  'wss://updates.sparkscan.io/connection/websocket';
const hintUrl = required('NEST_HINT_URL');
const hintSecret = required('SPARKSCAN_HINT_SECRET');
const registryUrl = required('NEST_REGISTRY_URL');
const registrySecret = required('WATCHER_REGISTRY_SECRET');
const controlSecret = required('WATCHER_CONTROL_SECRET');
const port = Number.parseInt(process.env.PORT ?? '3020', 10);
const outboxPath = process.env.OUTBOX_PATH?.trim() || 'data/outbox.sqlite';

const outbox = new SqliteOutbox(outboxPath);
const socket = new CentrifugeSocket(wsUrl);
let notifyAppend = (): void => undefined;
const watcher = new SparkWatcher({
  outbox,
  socket,
  network,
  now: () => Date.now(),
  deliver: (rawBody) =>
    deliverHint({ url: hintUrl, secret: hintSecret, rawBody }),
  onAppend: () => notifyAppend(),
  log: (line) => console.log(`[spark-watcher] ${line}`),
});
const delivery = startDeliveryLoop({
  flush: (limit) => watcher.flush(limit),
  nextDueAt: () => outbox.nextDueAt(),
  onError: (error) =>
    console.warn(
      `[spark-watcher] flush failed: ${error instanceof Error ? error.message : String(error)}`,
    ),
});
notifyAppend = () => delivery.onAppend();
delivery.kick();

socket.onDisconnect(() => watcher.onDisconnect());
socket.onConnect(() => watcher.onConnect());
socket.onSubscriptionError((channel, error) => {
  const detail =
    error && typeof error === 'object' && 'error' in error
      ? JSON.stringify(error.error)
      : String(error);
  console.warn(`[spark-watcher] sub error channel=${channel} ${detail}`);
});
socket.connect();

startControlServer({ port, secret: controlSecret, watcher });

// Pull the registry until Nest answers, then stop. Nest pushes later changes,
// and Nest also re-applies the full set when it boots.
void syncRegistryUntilConnected({
  url: registryUrl,
  secret: registrySecret,
  target: watcher,
  log: (line) => console.warn(`[spark-watcher] ${line}`),
});

const stats = setInterval(() => {
  console.log(`[spark-watcher] ${watcher.statsLine()}`);
  console.log(`[spark-watcher] ${watcher.watchedLine()}`);
}, 60_000);
stats.unref?.();

console.log(
  `[spark-watcher] listening on ${port} network=${network}`,
);

function required(name: string): string {
  const value = process.env[name]?.trim() ?? '';
  if (!value) throw new Error(`${name} is required`);
  return value;
}
