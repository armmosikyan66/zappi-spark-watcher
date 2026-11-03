import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, it } from 'node:test';
import { deliverHint } from './deliver.js';
import { signBody, verifyBody } from './hmac.js';
import { SqliteOutbox } from './sqlite-outbox.js';

describe('hmac', () => {
  it('accepts a fresh signature and rejects a skew', () => {
    const rawBody = '{"kind":"transaction"}';
    const timestamp = '1000';
    const signature = signBody(rawBody, 'secret', timestamp);
    assert.equal(
      verifyBody({
        rawBody,
        secret: 'secret',
        signature,
        timestamp,
        nowMs: 1000,
      }),
      true,
    );
    assert.equal(
      verifyBody({
        rawBody,
        secret: 'secret',
        signature,
        timestamp,
        nowMs: 1000 + 6 * 60 * 1000,
      }),
      false,
    );
  });
});

describe('deliverHint', () => {
  it('drops a 401 and retries a 503', async () => {
    const drop = await deliverHint({
      url: 'http://nest/hints',
      secret: 'secret',
      rawBody: '{}',
      fetchImpl: async () => new Response('no', { status: 401 }),
    });
    const retry = await deliverHint({
      url: 'http://nest/hints',
      secret: 'secret',
      rawBody: '{}',
      fetchImpl: async () => new Response('later', { status: 503 }),
    });
    assert.equal(drop, 'drop');
    assert.equal(retry, 'retry');
  });
});

describe('SqliteOutbox', () => {
  it('survives a reopen and deletes only after success', () => {
    const dir = mkdtempSync(join(tmpdir(), 'spark-watcher-'));
    const path = join(dir, 'outbox.sqlite');
    const first = new SqliteOutbox(path);
    first.append('{"kind":"transaction"}', 10);
    first.close();
    const second = new SqliteOutbox(path);
    const due = second.due(10, 10);
    assert.equal(due.length, 1);
    second.fail(due[0]!.id, 10, 50);
    assert.equal(second.due(10, 10).length, 0);
    assert.equal(second.due(60, 10).length, 1);
    assert.equal(second.nextDueAt(), 60);
    second.succeed(due[0]!.id);
    assert.equal(second.size(), 0);
    assert.equal(second.nextDueAt(), null);
    second.close();
  });
});
