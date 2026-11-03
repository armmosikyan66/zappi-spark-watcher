import type { WatchedAddress } from './registry.js';

const FIRST_DELAY_MS = 2_000;
const MAX_DELAY_MS = 30_000;

export type RegistryTarget = {
  apply(addresses: readonly WatchedAddress[]): void;
  watchedLine(): string;
};

/**
 * Pull the watched set once Nest answers, then stop. A refused connection
 * waits 2s, then 4s, up to 30s. This is a boot gate, not a poll.
 */
export async function syncRegistryUntilConnected(input: {
  url: string;
  secret: string;
  target: RegistryTarget;
  log: (line: string) => void;
  sleep?: (ms: number) => Promise<void>;
  fetchImpl?: typeof fetch;
}): Promise<void> {
  const sleep =
    input.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const fetchImpl = input.fetchImpl ?? fetch;
  let delay = FIRST_DELAY_MS;
  let attempt = 0;
  for (;;) {
    attempt += 1;
    const result = await pullRegistry(input.url, input.secret, fetchImpl);
    if (result.ok) {
      input.target.apply(result.addresses);
      input.log(
        `registry addresses=${result.addresses.length} ${input.target.watchedLine()}`,
      );
      return;
    }
    input.log(`registry retry attempt=${attempt} ${result.reason}`);
    await sleep(delay);
    delay = Math.min(delay * 2, MAX_DELAY_MS);
  }
}

async function pullRegistry(
  url: string,
  secret: string,
  fetchImpl: typeof fetch,
): Promise<
  | { ok: true; addresses: WatchedAddress[] }
  | { ok: false; reason: string }
> {
  try {
    const response = await fetchImpl(url, {
      headers: { authorization: `Bearer ${secret}` },
      signal: AbortSignal.timeout(8_000),
    });
    if (!response.ok) {
      return { ok: false, reason: `status=${response.status}` };
    }
    const body = (await response.json()) as { addresses?: WatchedAddress[] };
    return { ok: true, addresses: body.addresses ?? [] };
  } catch (error) {
    return { ok: false, reason: formatFetchError(error) };
  }
}

function formatFetchError(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const cause = error.cause;
  if (!cause || typeof cause !== 'object' || !('code' in cause)) {
    return error.message;
  }
  const code = String(cause.code);
  const address =
    'address' in cause && cause.address != null
      ? ` ${String(cause.address)}`
      : '';
  return `${error.message} (${code}${address})`;
}
