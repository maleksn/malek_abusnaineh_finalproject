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
const FLUSH_BATCH_SIZE = 5000;
const FLUSH_INTERVAL_MS = 5;
const MAX_QUEUE_CAPACITY = 20000;
const MAX_CONCURRENT_WORKERS = 4;

let chunkQueue: ValidLog[][] = [];
let queueHead = 0;
let totalQueuedLogs = 0;
let activeWorkers = 0;

/**
 * Enqueue logs in-memory and trigger concurrent workers
 */
export function enqueueLogs(logs: ValidLog[]): void {
  if (totalQueuedLogs + logs.length > MAX_QUEUE_CAPACITY) {
    throw new Error("Queue capacity exceeded");
  }
  chunkQueue.push(logs);
  totalQueuedLogs += logs.length;
  triggerWorkers();
}

function triggerWorkers(): void {
  while (activeWorkers < MAX_CONCURRENT_WORKERS && queueHead < chunkQueue.length) {
    activeWorkers++;
    void runWorker();
  }
}

async function runWorker(): Promise<void> {
  try {
    while (queueHead < chunkQueue.length) {
      const chunk = chunkQueue[queueHead++];
      if (!chunk || chunk.length === 0) continue;
      totalQueuedLogs -= chunk.length;

      // Free array memory immediately when drained
      if (queueHead >= chunkQueue.length) {
        chunkQueue = [];
        queueHead = 0;
        totalQueuedLogs = 0;
      }

      try {
        await insertLogsBulk(chunk);
      } catch (error) {
        console.error("Background COPY flush error:", error);
      }
    }
  } finally {
    activeWorkers--;
    if (queueHead < chunkQueue.length) {
      triggerWorkers();
    }
  }
}

// Periodic timer to flush any lingering logs in the queue
setInterval(() => {
  if (queueHead < chunkQueue.length) {
    triggerWorkers();
  }
}, FLUSH_INTERVAL_MS);
/**
 * High-speed bulk insertion using PostgreSQL COPY stream with low-GC chunked serialization
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
      const CHUNK_LOGS = 500;

      const writeNext = () => {
        while (index < logs.length) {
          const end = Math.min(index + CHUNK_LOGS, logs.length);
          const lines: string[] = [];
          for (; index < end; index++) {
            const log = logs[index]!;
            const attrJson =
              !log.attributes || Object.keys(log.attributes).length === 0
                ? '"{}"'
                : `"${JSON.stringify(log.attributes).replaceAll('"', '""')}"`;

            const msg =
              log.message.indexOf('"') === -1
                ? `"${log.message}"`
                : `"${log.message.replaceAll('"', '""')}"`;

            lines.push(
              `"${log.timestamp}","${log.level}","${log.service}",${msg},${attrJson}\n`,
            );
          }

          const chunk = lines.join("");
          if (!copyStream.write(chunk)) {
            copyStream.once("drain", writeNext);
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

