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

// ==========================================
// High-Throughput Background Ingest Pipeline
// ==========================================
const FLUSH_MAX_LOGS = 10000;
const FLUSH_INTERVAL_MS = 50;
const MAX_QUEUE_CAPACITY = 200000;
const COPY_CHUNK_SIZE = 65536; // 64 KB

let memoryQueue: ValidLog[] = [];
let isWorkerRunning = false;

/**
 * Enqueue logs in-memory and return immediately
 */
export function enqueueLogs(logs: ValidLog[]): void {
  if (memoryQueue.length + logs.length > MAX_QUEUE_CAPACITY) {
    throw new Error("Queue capacity exceeded");
  }

  for (let i = 0; i < logs.length; i++) {
    memoryQueue.push(logs[i]!);
  }

  if (!isWorkerRunning) {
    startBackgroundWorker();
  }
}

function startBackgroundWorker() {
  if (isWorkerRunning) return;
  isWorkerRunning = true;

  const processLoop = async () => {
    while (memoryQueue.length > 0) {
      const batchSize = Math.min(memoryQueue.length, FLUSH_MAX_LOGS);
      const batch = memoryQueue.splice(0, batchSize);

      try {
        await insertLogsBulk(batch);
      } catch (error) {
        console.error("Background COPY flush error:", error);
      }
    }

    isWorkerRunning = false;
  };

  void processLoop();
}

// Periodic timer to flush any lingering logs in the queue
setInterval(() => {
  if (memoryQueue.length > 0 && !isWorkerRunning) {
    startBackgroundWorker();
  }
}, FLUSH_INTERVAL_MS);

function csvField(value: string): string {
  if (value.indexOf('"') === -1) {
    return `"${value}"`;
  }

  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * High-speed bulk insertion using PostgreSQL COPY stream
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

          if (log === undefined) {
            continue;
          }

          const row =
            csvField(log.timestamp) +
            "," +
            csvField(log.level) +
            "," +
            csvField(log.service) +
            "," +
            csvField(log.message) +
            "," +
            csvField(JSON.stringify(log.attributes));

          buffer += row + "\n";

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
            copyStream.once("drain", () => {
              copyStream.end();
            });
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
    conditions.push(sql`${logsTable.attributes} ->> ${key} = ${value}`);
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
  data: AggregateBucketResult[];
  expiresAt: number;
}
const aggregateCache = new Map<string, CacheEntry>();

export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
): Promise<AggregateBucketResult[]> {
  // Check micro-cache (2-second TTL) to prevent thundering herd on single CPU
  const cacheKey = JSON.stringify({ query, attributeFilters });
  const now = Date.now();
  const cached = aggregateCache.get(cacheKey);

  if (cached !== undefined && cached.expiresAt > now) {
    return cached.data;
  }

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
    conditions.push(sql`${logsTable.attributes} ->> ${key} = ${value}`);
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

  const formattedResult: AggregateBucketResult[] = result.map((row) => ({
    start: row.start,
    group: row.group,
    count: row.count,
  }));

  // Cache result for 2 seconds
  aggregateCache.set(cacheKey, { data: formattedResult, expiresAt: now + 2000 });

  if (aggregateCache.size > 500) {
    for (const [key, entry] of aggregateCache) {
      if (entry.expiresAt <= now) {
        aggregateCache.delete(key);
      }
    }
  }

  return formattedResult;
}
