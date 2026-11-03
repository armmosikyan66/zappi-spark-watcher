export type OutboxRow = {
  id: number;
  body: string;
  attempts: number;
};

export interface Outbox {
  append(body: string, now: number): void;
  due(now: number, limit: number): OutboxRow[];
  /** Earliest next attempt, including rows that are not due yet. */
  nextDueAt(): number | null;
  succeed(id: number): void;
  fail(id: number, now: number, delayMs: number): void;
  size(): number;
}

type MemoryItem = OutboxRow & { nextAttemptAt: number };

export class MemoryOutbox implements Outbox {
  private nextId = 1;
  private readonly rows: MemoryItem[] = [];

  append(body: string, now: number): void {
    this.rows.push({
      id: this.nextId++,
      body,
      attempts: 0,
      nextAttemptAt: now,
    });
  }

  due(now: number, limit: number): OutboxRow[] {
    return this.rows
      .filter((row) => row.nextAttemptAt <= now)
      .slice(0, limit)
      .map(({ id, body, attempts }) => ({ id, body, attempts }));
  }

  nextDueAt(): number | null {
    if (this.rows.length === 0) return null;
    let earliest = this.rows[0]!.nextAttemptAt;
    for (const row of this.rows) {
      if (row.nextAttemptAt < earliest) earliest = row.nextAttemptAt;
    }
    return earliest;
  }

  succeed(id: number): void {
    const index = this.rows.findIndex((row) => row.id === id);
    if (index >= 0) this.rows.splice(index, 1);
  }

  fail(id: number, now: number, delayMs: number): void {
    const row = this.rows.find((item) => item.id === id);
    if (!row) return;
    row.attempts += 1;
    row.nextAttemptAt = now + delayMs;
  }

  size(): number {
    return this.rows.length;
  }
}
