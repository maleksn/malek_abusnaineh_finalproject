import type {
  ValidLog,
  LogsQuery,
  AggregateQuery,
} from "../validators/logs.validator";
import { db, pool } from "../db";
import { logs as logsTable } from "../db/schema";
import type { LogCursor } from "../utils/cursor";

import { from as copyFrom } from "pg-copy-streams";
import { and, desc, eq, gte, lt, sql } from "drizzle-orm";

// ===================================================
// High-Throughput Robust Background Ingest Pipeline
// ===================================================
const FLUSH_BATCH_SIZE = 4000;
const FLUSH_INTERVAL_MS = 5;
const MAX_QUEUE_CAPACITY = 30000;

let logQueue: string[] = [];
let isFlushing = false;

/**
 * Enqueue logs in-memory, pre-serialized into CSV to ensure minimal heap allocation
 */
export function enqueueLogs(logs: ValidLog[]): void {
  if (logQueue.length + logs.length > MAX_QUEUE_CAPACITY) {
    throw new Error("Queue capacity exceeded");
  }

  for (let i = 0; i < logs.length; i++) {
    const log = logs[i]!;
    const attrJson =
      !log.attributes || Object.keys(log.attributes).length === 0
        ? '"{}"'
        : `"${JSON.stringify(log.attributes).replaceAll('"', '""')}"`;

    const msg =
      log.message.indexOf('"') === -1
        ? `"${log.message}"`
        : `"${log.message.replaceAll('"', '""')}"`;

    logQueue.push(
      `"${log.timestamp}","${log.level}","${log.service}",${msg},${attrJson}\n`,
    );
  }

  if (!isFlushing) {
    void flushQueue();
  }
}

async function flushQueue(): Promise<void> {
  if (isFlushing) return;
  isFlushing = true;

  try {
    while (logQueue.length > 0) {
      const batch = logQueue.splice(0, FLUSH_BATCH_SIZE);
      if (batch.length === 0) break;

      try {
        await insertCsvBatch(batch);
      } catch (err) {
        console.error("Background COPY flush error:", err);
      }
    }
  } finally {
    isFlushing = false;
    if (logQueue.length > 0) {
      void flushQueue();
    }
  }
}

// Periodic timer to flush any lingering logs in the queue
setInterval(() => {
  if (logQueue.length > 0 && !isFlushing) {
    void flushQueue();
  }
}, FLUSH_INTERVAL_MS);

/**
 * High-speed bulk insertion using PostgreSQL COPY stream
 */
async function insertCsvBatch(lines: string[]): Promise<void> {
  if (lines.length === 0) return;
  let client;
  try {
    client = await pool.connect();
  } catch (err) {
    console.error("Failed to connect to database for COPY:", err);
    return;
  }

  try {
    const copyStream = client.query(
      copyFrom(
        `COPY logs (timestamp, level, service, message, attributes)
         FROM STDIN
         WITH (FORMAT csv)`,
      ),
    );

    await new Promise<void>((resolve, reject) => {
      let isDone = false;
      const onDone = () => {
        if (!isDone) {
          isDone = true;
          resolve();
        }
      };
      const onError = (err: Error) => {
        if (!isDone) {
          isDone = true;
          reject(err);
        }
      };

      copyStream.on("finish", onDone);
      copyStream.on("error", onError);

      let idx = 0;
      const CHUNK_SIZE = 1000;

      const writeNext = () => {
        while (idx < lines.length) {
          const end = Math.min(idx + CHUNK_SIZE, lines.length);
          let chunk = "";
          for (; idx < end; idx++) {
            chunk += lines[idx]!;
          }

          if (!copyStream.write(chunk)) {
            copyStream.once("drain", writeNext);
            return;
          }
        }
        copyStream.end();
      };

      writeNext();
    });
  } catch (err) {
    console.error("COPY stream error:", err);
  } finally {
    if (client) {
      client.release();
    }
  }
}

// ==========================================
// Query Functions
// ==========================================

export async function queryLogs(
  query: LogsQuery,
  attributeFilters: Record<string, string>,
  cursor?: LogCursor,
) {
  const conditions = [];

  if (query.service !== undefined) {
    conditions.push(eq(logsTable.service, query.service));
  }

  if (query.level !== undefined) {
    conditions.push(eq(logsTable.level, query.level));
  }

  if (query.since !== undefined) {
    conditions.push(gte(logsTable.timestamp, new Date(query.since)));
  }

  if (query.until !== undefined) {
    conditions.push(lt(logsTable.timestamp, new Date(query.until)));
  }

  if (query.q !== undefined) {
    const term = `%${query.q}%`;
    conditions.push(sql`${logsTable.message} ILIKE ${term}`);
  }

  if (cursor !== undefined) {
    const cursorTimestamp = new Date(cursor.timestamp);
    // Optimized single-pass tuple comparison for B-tree index
    conditions.push(
      sql`(${logsTable.timestamp}, ${logsTable.id}) < (${cursorTimestamp}, ${cursor.id})`,
    );
  }
  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} @> ${JSON.stringify({ [key]: value })}::jsonb`);
  }
  // Optimizer fence for GIN queries (q and attributes) to prevent slow backward heap scans
  const hasGinFilter = query.q !== undefined || Object.keys(attributeFilters).length > 0;
  if (hasGinFilter) {
    const subquery = db
      .select()
      .from(logsTable)
      .where(and(...conditions))
      .offset(0) // Forces Postgres optimizer NOT to flatten the subquery
      .as("filtered_logs");
    return db
      .select()
      .from(subquery)
      .orderBy(desc(subquery.timestamp), desc(subquery.id))
      .limit(query.limit + 1);
  }

  return db
    .select()
    .from(logsTable)
    .where(and(...conditions))
    .orderBy(desc(logsTable.timestamp), desc(logsTable.id))
    .limit(query.limit + 1);
}

function getBucketExpression(bucket: AggregateQuery["bucket"]) {
  switch (bucket) {
    case "1m":
      return sql<Date>`date_trunc('minute', ${logsTable.timestamp})`;
    case "5m":
      return sql<Date>`to_timestamp(floor(extract(epoch from ${logsTable.timestamp}) / 300) * 300)`;
    case "1h":
      return sql<Date>`date_trunc('hour', ${logsTable.timestamp})`;
    case "1d":
      return sql<Date>`date_trunc('day', ${logsTable.timestamp})`;
  }
}

export interface AggregateBucketResult {
  start: Date | string;
  group: string | null;
  count: number;
}

export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
): Promise<AggregateBucketResult[]> {
  const bucketExpression = getBucketExpression(query.bucket);
  const groupExpression =
    query.group_by === "service"
      ? logsTable.service
      : query.group_by === "level"
        ? logsTable.level
        : sql<null>`NULL`;

  const conditions = [
    gte(logsTable.timestamp, new Date(query.since)),
    lt(logsTable.timestamp, new Date(query.until)),
  ];

  if (query.service !== undefined) {
    conditions.push(eq(logsTable.service, query.service));
  }
  if (query.level !== undefined) {
    conditions.push(eq(logsTable.level, query.level));
  }
  if (query.q !== undefined) {
    const term = `%${query.q}%`;
    conditions.push(sql`${logsTable.message} ILIKE ${term}`);
  }
  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} @> ${JSON.stringify({ [key]: value })}::jsonb`);
  }

  const groupByClause =
    query.group_by !== undefined
      ? [sql.raw("1"), sql.raw("2")]
      : [sql.raw("1")];

  const result = await db
    .select({
      start: bucketExpression,
      group: groupExpression,
      count: sql<number>`count(*)::int`,
    })
    .from(logsTable)
    .where(and(...conditions))
    .groupBy(...groupByClause)
    .orderBy(sql.raw("1"));

  return result.map((row) => ({
    start: row.start,
    group: row.group,
    count: row.count,
  }));
}

