import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { startDeliveryLoop } from './delivery-loop.js';

type Timer = { id: number; ms: number; fn: () => void };

function harness(dueAt: number | null = null) {
  const timers: Timer[] = [];
  let seq = 1;
  let flushed = 0;
  let due = dueAt;
  const loop = startDeliveryLoop({
    flush: async () => {
      flushed += 1;
    },
    nextDueAt: () => due,
    now: () => 1_000,
    debounceMs: 25,
    schedule: (fn, ms) => {
      const id = seq++;
      timers.push({ id, ms, fn });
      return id;
    },
    clear: (id) => {
      const index = timers.findIndex((timer) => timer.id === id);
      if (index >= 0) timers.splice(index, 1);
    },
  });
  return {
    loop,
    timers,
    flushed: () => flushed,
    setDue: (value: number | null) => {
      due = value;
    },
  };
}

async function fire(timer: Timer): Promise<void> {
  timer.fn();
  await new Promise((resolve) => setImmediate(resolve));
}

describe('startDeliveryLoop', () => {
  it('coalesces a burst into one flush and does not poll', async () => {
    const { loop, timers, flushed } = harness(null);
    loop.onAppend();
    loop.onAppend();
    assert.equal(timers.length, 1);
    assert.equal(timers[0]?.ms, 25);
    assert.equal(flushed(), 0);
    await fire(timers[0]!);
    assert.equal(flushed(), 1);
    assert.equal(timers.length, 1);
  });

  it('schedules one retry at the earliest future attempt', async () => {
    const { loop, timers, flushed, setDue } = harness(6_000);
    loop.onAppend();
    await fire(timers[0]!);
    assert.equal(flushed(), 1);
    const retry = timers.find((timer) => timer.ms === 5_000);
    assert.ok(retry);
    setDue(null);
    await fire(retry);
    assert.equal(flushed(), 2);
    assert.equal(timers.filter((timer) => timer.ms === 5_000).length, 1);
  });

  it('kicks a due outbox without waiting for the next publication', async () => {
    const { loop, timers, flushed } = harness(500);
    loop.kick();
    assert.equal(timers[0]?.ms, 25);
    await fire(timers[0]!);
    assert.equal(flushed(), 1);
  });
});
