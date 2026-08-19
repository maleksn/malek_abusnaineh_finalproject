import type {
  ValidLog,
  LogsQuery,
  AggregateQuery,
} from "../validators/logs.validator";
import { pool, readPool } from "../db";
import type { LogCursor } from "../utils/cursor";

import { from as copyFrom } from "pg-copy-streams";
import { createHash } from "node:crypto";

// =========================================================================
// Dual-Worker Atomic Buffer Swap Ingestion Engine (Zero-OOM, Max Throughput)
// =========================================================================
type BatchResolver = {
  resolve: () => void;
  reject: (err: Error) => void;
};

const NUM_WORKERS = 3;
const workerBusy = [false, false, false];
let activeBuffer = "";
let pendingResolvers: BatchResolver[] = [];
let backpressureResolvers: (() => void)[] = [];
let activeWorkerPromises: Promise<void>[] = [];
const FLUSH_THRESHOLD_BYTES = 64 * 1024; // 64KB (~500 logs)
const MAX_BUFFER_BACKPRESSURE_BYTES = 128 * 1024; // 128KB backpressure limit (~1,000 logs max)

/**
 * Instant consistency barrier for read-after-write queries.
 * Flushes any pending logs in activeBuffer and awaits all in-flight worker commits.
 */
export async function flushCurrentBuffer(): Promise<void> {
  while (activeBuffer.length > 0 || activeWorkerPromises.length > 0) {
    if (activeBuffer.length > 0) dispatch();
    if (activeWorkerPromises.length > 0) {
      await Promise.allSettled([...activeWorkerPromises]);
    }
    // Yield to event loop to allow worker 'finally' blocks to execute and update the promises array
    await new Promise((resolve) => setImmediate(resolve));
  }
}

let dispatchScheduled = false;

function scheduleDispatch(): void {
  if (activeBuffer.length >= FLUSH_THRESHOLD_BYTES) {
    dispatch();
    return;
  }
  if (!dispatchScheduled) {
    dispatchScheduled = true;
    queueMicrotask(() => {
      dispatchScheduled = false;
      dispatch();
    });
  }
}

/**
 * Enqueue a pre-serialized CSV string chunk directly.
 * Returns a Promise that resolves when the chunk is durably committed to PostgreSQL,
 * or rejects if database insertion fails (guaranteeing FR-09 durability).
 *
 * Backpressure: If activeBuffer exceeds MAX_BUFFER_BACKPRESSURE_BYTES,
 * await ongoing buffer swap before accepting more data to prevent unbounded memory growth.
 */
export async function enqueueCsvChunk(chunk: string): Promise<void> {
  if (!chunk) return;

  if (activeBuffer.length >= MAX_BUFFER_BACKPRESSURE_BYTES) {
    await new Promise<void>((resolve) => backpressureResolvers.push(resolve));
  }

  return new Promise<void>((resolve, reject) => {
    activeBuffer += chunk;
    pendingResolvers.push({ resolve, reject });

    scheduleDispatch();
  });
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
      // Synchronously and atomically swap activeBuffer and pendingResolvers
      const payload = activeBuffer;
      const resolvers = pendingResolvers;
      activeBuffer = "";
      pendingResolvers = [];

      // Notify any awaiting backpressure requests that buffer has been swapped
      if (backpressureResolvers.length > 0) {
        const bpResolvers = backpressureResolvers;
        backpressureResolvers = [];
        for (let r = 0; r < bpResolvers.length; r++) {
          bpResolvers[r]!();
        }
      }

      const flushPromise = insertCsvPayload(payload);
      activeWorkerPromises.push(flushPromise);

      try {
        await flushPromise;

        // Durable commit successful: resolve all callers for this batch
        for (let r = 0; r < resolvers.length; r++) {
          resolvers[r]!.resolve();
        }
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        console.error(`Worker ${id} COPY flush error:`, error.message);
        // Insertion failed: reject all callers for this batch to prevent silent data loss (FR-09)
        for (let r = 0; r < resolvers.length; r++) {
          resolvers[r]!.reject(error);
        }
      } finally {
        const idx = activeWorkerPromises.indexOf(flushPromise);
        if (idx !== -1) activeWorkerPromises.splice(idx, 1);
      }
    }
  } finally {
    workerBusy[id] = false;
    if (activeBuffer.length > 0) {
      dispatch();
    }
  }
}

/**
 * High-speed bulk insertion using PostgreSQL COPY stream.
 * Propagates all errors to caller and ensures proper client cleanup on error.
 */
async function insertCsvPayload(payload: string): Promise<void> {
  if (!payload) return;
  const client = await pool.connect();
  let streamError: Error | null = null;

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

    await client.query("SELECT 1");
  } catch (err) {
    streamError = err instanceof Error ? err : new Error(String(err));
    throw streamError;
  } finally {
    client.release(streamError || undefined);
  }
}

// ==========================================
export async function queryLogs(
  query: LogsQuery,
  attributeFilters: Record<string, string>,
  cursor?: LogCursor,
) {
  const params: (string | number | Date)[] = [];
  const whereClauses: string[] = [];

  if (query.service !== undefined) {
    params.push(query.service);
    whereClauses.push(`service = $${params.length}`);
  }

  if (query.level !== undefined) {
    params.push(query.level);
    whereClauses.push(`level = $${params.length}`);
  }

  if (query.since !== undefined) {
    params.push(new Date(query.since));
    whereClauses.push(`timestamp >= $${params.length}`);
  }

  if (query.until !== undefined) {
    params.push(new Date(query.until));
    whereClauses.push(`timestamp < $${params.length}`);
  }

  if (query.q !== undefined) {
    params.push(`%${query.q}%`);
    whereClauses.push(`message ILIKE $${params.length}`);
  }

  if (cursor !== undefined) {
    params.push(new Date(cursor.timestamp), cursor.id);
    whereClauses.push(`(timestamp, id) < ($${params.length - 1}, $${params.length})`);
  }

  const attrEntries = Object.entries(attributeFilters);
  for (const [key, value] of attrEntries) {
    params.push(key, value);
    whereClauses.push(`attributes->>$${params.length - 1} = $${params.length}`);
  }

  params.push(query.limit + 1);
  const limitParam = `$${params.length}`;

  const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
  const hasGinFilter = query.q !== undefined || attrEntries.length > 0;

  // Use optimizer fence (OFFSET 0) for GIN-filtered queries to force GIN index usage instead of backward B-Tree scan
  const sqlText = hasGinFilter
    ? `
      SELECT id, timestamp, level, service, message, attributes
      FROM (
        SELECT id, timestamp, level, service, message, attributes
        FROM logs
        ${whereSql}
        ORDER BY timestamp DESC, id DESC
        OFFSET 0
      ) filtered
      ORDER BY timestamp DESC, id DESC
      LIMIT ${limitParam}
    `
    : `
      SELECT id, timestamp, level, service, message, attributes
      FROM logs
      ${whereSql}
      ORDER BY timestamp DESC, id DESC
      LIMIT ${limitParam}
    `;

  const queryName = "q_logs_" + createHash("md5").update(sqlText).digest("hex").slice(0, 16);

  const res = await readPool.query<{
    id: number | string;
    timestamp: Date | string;
    level: "debug" | "info" | "warn" | "error";
    service: string;
    message: string;
    attributes: Record<string, unknown>;
  }>({
    name: queryName,
    text: sqlText,
    values: params,
  });

  return res.rows.map((row) => ({
    id: String(row.id),
    timestamp:
      typeof row.timestamp === "string"
        ? row.timestamp
        : (row.timestamp instanceof Date ? row.timestamp.toISOString() : String(row.timestamp)),
    level: row.level,
    service: row.service,
    message: row.message,
    attributes: row.attributes ?? {},
  }));
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

    const queryName = `agg_fast_${query.bucket}_${query.group_by || "none"}_${query.service !== undefined ? "s" : "_"}_${query.level !== undefined ? "l" : "_"}`;

    const res = await readPool.query<{ start: Date | string; group: string | null; count: number }>({
      name: queryName,
      text: sqlText,
      values: params,
    });
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

  if (query.q !== undefined) {
    params.push(`%${query.q}%`);
    whereClauses.push(`message ILIKE $${params.length}`);
  }

  for (const [key, value] of Object.entries(attributeFilters)) {
    params.push(key, value);
    whereClauses.push(`attributes->>$${params.length - 1} = $${params.length}`);
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

  const res = await readPool.query<{ start: Date | string; group: string | null; count: number }>({
    text: sqlText,
    values: params,
  });

  return res.rows;
}
