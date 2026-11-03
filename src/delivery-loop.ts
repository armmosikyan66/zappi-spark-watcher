/**
 * One debounce for new hints, one timer for the next retry. No interval.
 */
export type DeliveryLoop = {
  onAppend: () => void;
  kick: () => void;
  stop: () => void;
};

const DEFAULT_DEBOUNCE_MS = 25;
const DEFAULT_BATCH = 100;

export function startDeliveryLoop(input: {
  flush: (limit: number) => Promise<void>;
  nextDueAt: () => number | null;
  now?: () => number;
  debounceMs?: number;
  batch?: number;
  schedule?: (fn: () => void, ms: number) => number;
  clear?: (id: number) => void;
  onError?: (error: unknown) => void;
}): DeliveryLoop {
  const now = input.now ?? (() => Date.now());
  const debounceMs = input.debounceMs ?? DEFAULT_DEBOUNCE_MS;
  const batch = input.batch ?? DEFAULT_BATCH;
  const schedule =
    input.schedule ??
    ((fn, ms) => {
      const timer = setTimeout(fn, ms);
      timer.unref?.();
      return timer as unknown as number;
    });
  const clear =
    input.clear ??
    ((id) => {
      clearTimeout(id as unknown as ReturnType<typeof setTimeout>);
    });
  const onError = input.onError ?? (() => undefined);

  let flushTimer: number | null = null;
  let retryTimer: number | null = null;
  let flushing = false;
  let queued = false;
  let stopped = false;

  const requestFlush = (): void => {
    if (stopped || flushTimer != null) return;
    flushTimer = schedule(() => {
      flushTimer = null;
      void runFlush();
    }, debounceMs);
  };

  const armRetry = (): void => {
    if (retryTimer != null) {
      clear(retryTimer);
      retryTimer = null;
    }
    if (stopped) return;
    const dueAt = input.nextDueAt();
    if (dueAt == null) return;
    const delay = Math.max(0, dueAt - now());
    if (delay === 0) {
      requestFlush();
      return;
    }
    retryTimer = schedule(() => {
      retryTimer = null;
      void runFlush();
    }, delay);
  };

  const runFlush = async (): Promise<void> => {
    if (stopped) return;
    if (flushing) {
      queued = true;
      return;
    }
    flushing = true;
    try {
      do {
        queued = false;
        await input.flush(batch);
      } while (queued && !stopped);
    } catch (error) {
      onError(error);
    } finally {
      flushing = false;
    }
    armRetry();
  };

  return {
    onAppend: requestFlush,
    kick: armRetry,
    stop: () => {
      stopped = true;
      if (flushTimer != null) clear(flushTimer);
      if (retryTimer != null) clear(retryTimer);
      flushTimer = null;
      retryTimer = null;
    },
  };
}
