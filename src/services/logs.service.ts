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

// =========================================================================
// Dual-Worker Atomic Buffer Swap Ingestion Engine (Zero-OOM, Max Throughput)
// =========================================================================
const NUM_WORKERS = 3;
const workerBusy = [false, false, false];
let activeBuffer = "";
let flushResolvers: (() => void)[] = [];

const FLUSH_THRESHOLD_BYTES = 64 * 1024; // 64KB (~500-800 logs)
const MAX_BUFFER_BACKPRESSURE_BYTES = 4 * 1024 * 1024; // 4MB backpressure limit

/**
 * Enqueue a pre-serialized CSV string chunk directly.
 * Backpressure: If activeBuffer exceeds MAX_BUFFER_BACKPRESSURE_BYTES,
 * await the ongoing flush before accepting more data to prevent unbounded memory growth.
 */
export async function enqueueCsvChunk(chunk: string): Promise<void> {
  if (activeBuffer.length >= MAX_BUFFER_BACKPRESSURE_BYTES) {
    await new Promise<void>((resolve) => flushResolvers.push(resolve));
  }

  activeBuffer += chunk;
  dispatch();
}

function dispatch(): void {
  for (let i = 0; i < NUM_WORKERS; i++) {
    if (!workerBusy[i] && activeBuffer.length > 0) {
      void runWorker(i);
    }
  }
}

async function runWorker(id: number): Promise<void> {
  if (workerBusy[id]) return;
  workerBusy[id] = true;

  try {
    while (activeBuffer.length > 0) {
      // Synchronously and atomically swap activeBuffer before any async operation
      const payload = activeBuffer;
      activeBuffer = "";

      // Notify any awaiting requests that buffer has been swapped
      if (flushResolvers.length > 0) {
        const resolvers = flushResolvers;
        flushResolvers = [];
        for (let r = 0; r < resolvers.length; r++) resolvers[r]!();
      }

      try {
        await insertCsvPayload(payload);
      } catch (err) {
        console.error(`Worker ${id} COPY flush error:`, err);
      }
    }
  } finally {
    workerBusy[id] = false;
    if (activeBuffer.length > 0) {
      dispatch();
    }
  }
}

// 5ms low-latency timer to dispatch any trailing logs in the buffer
setInterval(() => {
  if (activeBuffer.length > 0) {
    dispatch();
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

  // Ultra-fast path for read-after-write (GET /logs?limit=20)
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

  // Fast path for service filtered query with cursor or default pagination
  const isServiceOnly =
    query.service !== undefined &&
    query.level === undefined &&
    query.since === undefined &&
    query.until === undefined &&
    query.q === undefined &&
    !hasAttrFilters;

  if (isServiceOnly) {
    if (cursor !== undefined) {
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
         WHERE service = $1 AND (timestamp, id) < ($2, $3)
         ORDER BY service, timestamp DESC, id DESC
         LIMIT $4`,
        [query.service, new Date(cursor.timestamp), cursor.id, query.limit + 1],
      );
      return res.rows;
    } else {
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
         WHERE service = $1
         ORDER BY service, timestamp DESC, id DESC
         LIMIT $2`,
        [query.service, query.limit + 1],
      );
      return res.rows;
    }
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
    conditions.push(
      sql`(${logsTable.timestamp}, ${logsTable.id}) < (${cursorTimestamp}, ${cursor.id})`,
    );
  }
  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} @> ${JSON.stringify({ [key]: value })}::jsonb`);
  }

  // Optimizer fence for GIN queries (q and attributes) to prevent slow backward heap scans
  const hasGinFilter = query.q !== undefined || hasAttrFilters;
  if (hasGinFilter) {
    const subquery = db
      .select()
      .from(logsTable)
      .where(and(...conditions))
      .offset(0)
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

  // Pure Index-Only Scan path on logs_timestamp_service_level_idx
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

    let bucketSql = "date_bin('1 minute'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
    if (query.bucket === "5m") {
      bucketSql = "date_bin('5 minutes'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
    } else if (query.bucket === "1h") {
      bucketSql = "date_bin('1 hour'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
    } else if (query.bucket === "1d") {
      bucketSql = "date_bin('1 day'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
    }

    const groupCol =
      query.group_by === "service"
        ? "service"
        : query.group_by === "level"
          ? "level::text"
          : "NULL::text";

    const groupBySql =
      query.group_by !== undefined
        ? "GROUP BY 1, 2 ORDER BY 1, 2"
        : "GROUP BY 1 ORDER BY 1";

    const sqlText = `
      SELECT ${bucketSql} AS start, ${groupCol} AS "group", count(*)::int AS count
      FROM logs
      WHERE ${whereClauses.join(" AND ")}
      ${groupBySql}
    `;

    const res = await pool.query<{ start: Date | string; group: string | null; count: number }>(
      sqlText,
      params,
    );
    return res.rows;
  }

  // Fallback for complex queries with q or attributes
  let bucketSql = "date_bin('1 minute'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
  if (query.bucket === "5m") {
    bucketSql = "date_bin('5 minutes'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
  } else if (query.bucket === "1h") {
    bucketSql = "date_bin('1 hour'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
  } else if (query.bucket === "1d") {
    bucketSql = "date_bin('1 day'::interval, timestamp, TIMESTAMPTZ '2000-01-01 00:00:00Z')";
  }

  const groupCol =
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
      start: sql<Date>`${sql.raw(bucketSql)}`,
      group: groupCol,
      count: sql<number>`count(*)::int`,
    })
    .from(logsTable)
    .where(and(...conditions))
    .groupBy(...groupByClause)
    .orderBy(sql.raw("1"));

  return result.map((row) => ({
    start: row.start,
    group: row.group as string | null,
    count: row.count,
  }));
}


