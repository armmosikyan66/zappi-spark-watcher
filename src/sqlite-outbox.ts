import Database from 'better-sqlite3';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Outbox, OutboxRow } from './outbox.js';

export class SqliteOutbox implements Outbox {
  private readonly db: Database.Database;
  private readonly insertRow: Database.Statement;
  private readonly selectDue: Database.Statement;
  private readonly deleteRow: Database.Statement;
  private readonly markFailed: Database.Statement;
  private readonly countRows: Database.Statement;
  private readonly earliestDue: Database.Statement;

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        body TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS outbox_next_attempt ON outbox (next_attempt_at);
    `);
    this.insertRow = this.db.prepare(
      'INSERT INTO outbox (body, attempts, next_attempt_at) VALUES (?, 0, ?)',
    );
    this.selectDue = this.db.prepare(
      `SELECT id, body, attempts FROM outbox
       WHERE next_attempt_at <= ?
       ORDER BY id ASC
       LIMIT ?`,
    );
    this.deleteRow = this.db.prepare('DELETE FROM outbox WHERE id = ?');
    this.markFailed = this.db.prepare(
      `UPDATE outbox
       SET attempts = attempts + 1, next_attempt_at = ?
       WHERE id = ?`,
    );
    this.countRows = this.db.prepare('SELECT COUNT(*) AS n FROM outbox');
    this.earliestDue = this.db.prepare(
      'SELECT MIN(next_attempt_at) AS due FROM outbox',
    );
  }

  append(body: string, now: number): void {
    this.insertRow.run(body, now);
  }

  due(now: number, limit: number): OutboxRow[] {
    return this.selectDue.all(now, limit) as OutboxRow[];
  }

  nextDueAt(): number | null {
    const row = this.earliestDue.get() as { due: number | null };
    return row.due;
  }

  succeed(id: number): void {
    this.deleteRow.run(id);
  }

  fail(id: number, now: number, delayMs: number): void {
    this.markFailed.run(now + delayMs, id);
  }

  size(): number {
    const row = this.countRows.get() as { n: number };
    return row.n;
  }

  close(): void {
    this.db.close();
  }
}
