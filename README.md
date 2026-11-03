# spark-watcher

Separate process that holds the Sparkscan WebSocket. Nest deploys do not drop that socket. The watcher does not write the domain database and does not accept browser connections.

## Stack

Node.js 20, TypeScript, `centrifuge` v5 (JSON frames), `better-sqlite3` WAL outbox, `node:crypto` HMAC, `node:http` control server.

## Run

```bash
npm install
npm run build
npm start
```

| Env | Required | Meaning |
| --- | --- | --- |
| `NEST_HINT_URL` | yes | `POST` target, Nest `/api/wallet/sparkscan/hints` |
| `SPARKSCAN_HINT_SECRET` | yes | HMAC secret shared with Nest `SPARKSCAN_HINT_SECRET` |
| `NEST_REGISTRY_URL` | yes | Nest `GET /api/internal/watcher/addresses` |
| `WATCHER_REGISTRY_SECRET` | yes | Bearer token for that route |
| `WATCHER_CONTROL_SECRET` | yes | HMAC secret for `POST /control/subscriptions` |
| `PORT` | no | Control and `/healthz` port. Default `3020` |
| `SPARKSCAN_WS_URL` | no | Default `wss://updates.sparkscan.io/connection/websocket` |
| `SPARKSCAN_NETWORK` | no | Default `MAINNET` |
| `OUTBOX_PATH` | no | SQLite file. Default `data/outbox.sqlite`. Put it on a volume |

Startup pulls `NEST_REGISTRY_URL` until Nest answers (2s, then 4s, up to 30s) and then stops. Start the watcher before or after Nest; a refused registry is retried, not given up. Nest also posts the full live set to `POST /control/subscriptions` (`action: apply`) when Nest itself boots, so a Nest restart repairs a drifted subscription list. Later address changes stay push-only. `GET /healthz` returns `{ ok, held }`.

## Channels

Each held wallet is two Sparkscan subscriptions. The channel name uses the `sp1` identifier: the `spark1` receive address with the bech32m HRP swapped from `spark` to `sp` and the checksum recomputed.

- `/transaction/in/{network}/{sp1}` — incoming
- `/transaction/out/{network}/{sp1}` — outgoing

There is no `/transaction/network/{network}` subscription. A publication on either channel becomes one hint. The hint still carries the `spark1` address so Nest can resolve the user. An address that will not decode is skipped and logged. A refused subscribe is logged as `sub error channel=…`.

Session scope is unchanged: Nest subscribes a wallet when that user has a live socket and unsubscribes it when the last tab drops.

Hints flush when a publication lands (25 ms debounce, keep-alive HTTP to Nest). A failed delivery schedules one timer at its next attempt. There is no flush interval.

Railway: one service in the same region as Nest, start command `npm start`, health check `GET /healthz`. Do not attach a public domain. Nest reaches the watcher, and the watcher reaches Nest, on the private network (`*.railway.internal`). Persist `OUTBOX_PATH` on a volume.

Delivery: `2xx` deletes the outbox row. `401`, `400`, and `413` are terminal (a bad signature will not become valid). Anything else retries with backoff. A Nest deploy is a retry, not a loss.
