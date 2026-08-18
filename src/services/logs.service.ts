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
// Single-Stream Dedicated High-Throughput Ingest Engine
// ===================================================
const FLUSH_CHUNK_BATCH = 40; // 40 chunks = 4,000 logs per COPY batch
const MAX_QUEUE_LOGS = 100000;

let logQueue: string[] = [];
let queuedLogsCount = 0;
let isFlushing = false;

/**
 * Enqueue a pre-serialized CSV chunk directly into in-memory queue
 */
export function enqueueCsvChunk(chunk: string, count: number): void {
  if (queuedLogsCount + count > MAX_QUEUE_LOGS) {
    throw new Error("Queue capacity exceeded");
  }

  logQueue.push(chunk);
  queuedLogsCount += count;

  if (!isFlushing) {
    void runFlushLoop();
  }
}

async function runFlushLoop(): Promise<void> {
  if (isFlushing) return;
  isFlushing = true;

  try {
    while (logQueue.length > 0) {
      const batch = logQueue.splice(0, FLUSH_CHUNK_BATCH);
      const batchPayload = batch.join("");

      try {
        await insertCsvPayload(batchPayload);
      } catch (err) {
        console.error("Background COPY flush error:", err);
      }

      queuedLogsCount = Math.max(0, queuedLogsCount - batch.length * 100);
    }
  } finally {
    isFlushing = false;
    if (logQueue.length > 0) {
      void runFlushLoop();
    }
  }
}

// Periodic timer to flush any lingering logs in the queue
setInterval(() => {
  if (logQueue.length > 0 && !isFlushing) {
    void runFlushLoop();
  }
}, 5);

/**
 * High-speed bulk insertion using PostgreSQL COPY stream
 */
async function insertCsvPayload(payload: string): Promise<void> {
  if (!payload) return;
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
      copyStream.on("finish", resolve);
      copyStream.on("error", reject);

      copyStream.end(payload);
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
  const hasAttrFilters = Object.keys(attributeFilters).length > 0;
  const isSimpleReadAfterWrite =
    query.service === undefined &&
    query.level === undefined &&
    query.since === undefined &&
    query.until === undefined &&
    query.q === undefined &&
    cursor === undefined &&
    !hasAttrFilters;

  if (isSimpleReadAfterWrite) {
    const res = await pool.query<{
      id: number;
      timestamp: Date;
      level: "debug" | "info" | "warn" | "error";
      service: string;
      message: string;
      attributes: Record<string, unknown>;
      createdAt: Date;
    }>(
      `SELECT id, timestamp, level, service, message, attributes, created_at AS "createdAt"
       FROM logs
       ORDER BY timestamp DESC, id DESC
       LIMIT $1`,
      [query.limit + 1],
    );
    return res.rows;
  }

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
  const hasAttrFilters = Object.keys(attributeFilters).length > 0;
  
  // Fast path for standard bucket aggregate queries using direct pg pool query
  if (!hasAttrFilters && query.q === undefined) {
    const params: (string | Date)[] = [new Date(query.since), new Date(query.until)];
    const whereClauses: string[] = ["timestamp >= $1", "timestamp < $2"];

    if (query.service !== undefined) {
      params.push(query.service);
      whereClauses.push(`service = $${params.length}`);
    }

    if (query.level !== undefined) {
      params.push(query.level);
      whereClauses.push(`level = $${params.length}`);
    }

    let bucketSql = "date_trunc('minute', timestamp)";
    if (query.bucket === "5m") {
      bucketSql = "to_timestamp(floor(extract(epoch from timestamp) / 300) * 300)";
    } else if (query.bucket === "1h") {
      bucketSql = "date_trunc('hour', timestamp)";
    } else if (query.bucket === "1d") {
      bucketSql = "date_trunc('day', timestamp)";
    }

    const groupCol = query.group_by === "service" ? "service" : query.group_by === "level" ? "level" : "NULL::text";
    const groupBySql = query.group_by !== undefined ? "GROUP BY 1, 2 ORDER BY 1" : "GROUP BY 1 ORDER BY 1";

    const sqlText = `
      SELECT ${bucketSql} AS start, ${groupCol} AS "group", count(*)::int AS count
      FROM logs
      WHERE ${whereClauses.join(" AND ")}
      ${groupBySql}
    `;

    const res = await pool.query<{ start: Date | string; group: string | null; count: number }>(sqlText, params);
    return res.rows;
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

