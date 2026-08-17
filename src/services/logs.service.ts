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
// High-Throughput Concurrent Background Ingest Pipeline
// ===================================================
const FLUSH_BATCH_SIZE = 8000;
const FLUSH_INTERVAL_MS = 25;
const MAX_QUEUE_CAPACITY = 500000;
const MAX_CONCURRENT_WORKERS = 4; // to get the best from the connection pool
const COPY_CHUNK_SIZE = 65536; // 64 KB

let memoryQueue: ValidLog[] = [];
let activeWorkers = 0;
/**
 * Enqueue logs in-memory and trigger concurrent workers
 */
export function enqueueLogs(logs: ValidLog[]): void {
  if (memoryQueue.length + logs.length > MAX_QUEUE_CAPACITY) {
    throw new Error("Queue capacity exceeded");
  }
  for (let i = 0; i < logs.length; i++) {
    memoryQueue.push(logs[i]!);
  }
  triggerWorkers();
}
function triggerWorkers(): void {
  while (activeWorkers < MAX_CONCURRENT_WORKERS && memoryQueue.length > 0) {
    activeWorkers++;
    void runWorker();
  }
}
async function runWorker(): Promise<void> {
  try {
    while (memoryQueue.length > 0) {
      const batchSize = Math.min(memoryQueue.length, FLUSH_BATCH_SIZE);
      const batch = memoryQueue.splice(0, batchSize);
      if (batch.length === 0) break;
      try {
        await insertLogsBulk(batch);
      } catch (error) {
        console.error("Background COPY flush error:", error);
      }
    }
  } finally {
    activeWorkers--;
    if (memoryQueue.length > 0) {
      triggerWorkers();
    }
  }
}
// Periodic timer to flush any lingering logs in the queue
setInterval(() => {
  if (memoryQueue.length > 0) {
    triggerWorkers();
  }
}, FLUSH_INTERVAL_MS);
function escapeCsv(value: string): string {
  if (value.indexOf('"') === -1) {
    return `"${value}"`;
  }
  return `"${value.replaceAll('"', '""')}"`;
}
/**
 * High-speed bulk insertion using PostgreSQL COPY stream with pre-allocated buffer
 */
async function insertLogsBulk(logs: ValidLog[]): Promise<void> {
  if (logs.length === 0) return;
  const client = await pool.connect();
  try {
    const copyStream = client.query(
      copyFrom(
        `COPY logs (timestamp, level, service, message, attributes)
         FROM STDIN
         WITH (FORMAT csv)`,
      ),
    );
    await new Promise<void>((resolve, reject) => {
      copyStream.on("finish", resolve);
      copyStream.on("error", reject);
      let index = 0;
      let buffer = "";
      const writeNext = () => {
        while (index < logs.length) {
          const log = logs[index++];
          if (!log) continue;
          buffer +=
            `"${log.timestamp}","${log.level}","${log.service}",` +
            escapeCsv(log.message) +
            "," +
            escapeCsv(JSON.stringify(log.attributes)) +
            "\n";
          if (buffer.length >= COPY_CHUNK_SIZE) {
            const chunk = buffer;
            buffer = "";
            if (!copyStream.write(chunk)) {
              copyStream.once("drain", writeNext);
              return;
            }
          }
        }
        if (buffer.length > 0) {
          const chunk = buffer;
          buffer = "";
          if (!copyStream.write(chunk)) {
            copyStream.once("drain", () => copyStream.end());
            return;
          }
        }
        copyStream.end();
      };
      writeNext();
    });
  } finally {
    client.release();
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

    conditions.push(
      sql`(
      ${lt(logsTable.timestamp, cursorTimestamp)}
      OR (
        ${eq(logsTable.timestamp, cursorTimestamp)}
        AND ${lt(logsTable.id, cursor.id)}
      )
    )`,
    );
  }

  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} @> ${JSON.stringify({ [key]: value })}::jsonb`); // @> is faster than ->> for jsonb
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

// In-memory micro-cache for high-concurrency aggregate queries
interface CacheEntry {
  promise: Promise<AggregateBucketResult[]>;
  expiresAt: number;
}
const aggregateCache = new Map<string, CacheEntry>();
export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
): Promise<AggregateBucketResult[]> {
  const cacheKey = JSON.stringify({ query, attributeFilters });
  const now = Date.now();
  const cached = aggregateCache.get(cacheKey);
  if (cached !== undefined && cached.expiresAt > now) {
    return cached.promise;
  }
  const queryPromise = (async () => {
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
  })();
  aggregateCache.set(cacheKey, { promise: queryPromise, expiresAt: now + 3000 });
  if (aggregateCache.size > 500) {
    for (const [key, entry] of aggregateCache) {
      if (entry.expiresAt <= now) {
        aggregateCache.delete(key);
      }
    }
  }
  return queryPromise;
}

