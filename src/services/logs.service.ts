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
// Group-Commit Ingestion Engine (ack only after PostgreSQL commit)
// =========================================================================

interface PendingPayload {
  data: string;
  bytes: number;
  resolve: () => void;
  reject: (err: Error) => void;
}

// One worker per connection in the write pool (pool.max = 3 or 4)
const NUM_WORKERS = 3;

// Memory cap for uncommitted pending data (512 KB)
const MAX_PENDING_BYTES = 512 * 1024;

// Size limit for a single COPY command: bounded prefix from the queue.
// Keeps individual COPY operations fast and allows workers to share bursts in parallel.
const MAX_COPY_BYTES = 128 * 1024;

const workerBusy: boolean[] = new Array(NUM_WORKERS).fill(false);

let queue: PendingPayload[] = [];
let pendingBytes = 0;
let drainWaiters: (() => void)[] = [];

function dispatch(): void {
  for (let i = 0; i < NUM_WORKERS; i++) {
    if (!workerBusy[i] && queue.length > 0) void runWorker(i);
  }
}

function takeBatch(): PendingPayload[] {
  let cut = 0;
  let bytes = 0;
  while (cut < queue.length) {
    const next = queue[cut]!;
    if (cut > 0 && bytes + next.bytes > MAX_COPY_BYTES) break;
    bytes += next.bytes;
    cut++;
  }
  const batch = queue.slice(0, cut);
  queue = queue.slice(cut);
  pendingBytes -= bytes;
  return batch;
}

async function runWorker(id: number): Promise<void> {
  if (workerBusy[id]) return;
  workerBusy[id] = true;
  try {
    while (queue.length > 0) {
      const batch = takeBatch();
      const data = batch.map((p) => p.data).join("");

      try {
        await insertCsvPayload(data);
        for (const p of batch) p.resolve();
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        console.error(`Worker ${id} COPY flush error:`, error.message);
        for (const p of batch) p.reject(error);
      } finally {
        // Paced admission: wake up only as many requests as were committed in this batch.
        // Admission rate matches actual database commit rate to keep queue small
        // and eliminate thundering herds.
        const wake = Math.min(batch.length, drainWaiters.length);
        for (let i = 0; i < wake; i++) {
          const w = drainWaiters.shift();
          if (w) w();
        }
      }
    }
  } finally {
    workerBusy[id] = false;
    if (queue.length > 0) dispatch();
  }
}

export async function enqueueCsvChunk(chunk: string): Promise<void> {
  if (!chunk) return;

  while (pendingBytes >= MAX_PENDING_BYTES) {
    await new Promise<void>((r) => drainWaiters.push(r));
  }

  await new Promise<void>((resolve, reject) => {
    pendingBytes += chunk.length;
    queue.push({ data: chunk, bytes: chunk.length, resolve, reject });
    dispatch();
  });
}

export async function flushCurrentBuffer(): Promise<void> {
  while (queue.length > 0 || workerBusy.some((b) => b)) {
    await new Promise((r) => setImmediate(r));
  }
}

/**
 * DATA FLOW: High-speed bulk insertion directly into PostgreSQL table.
 * Uses PostgreSQL COPY stream for maximum speed.
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

// =========================================================================
// Query Engine (Reading and filtering logs from the database)
// =========================================================================

/**
 * DATA FLOW: Searches and retrieves logs based on user filters.
 * Step 1: Collect user filters (service name, log level, time range, search text, custom attributes).
 * Step 2: If a pagination cursor is provided, fetch logs older than that cursor.
 * Step 3: Build the SQL query and execute it on the read database pool.
 * Step 4: Format and return the matching log entries to the caller.
 */
export async function queryLogs(
  query: LogsQuery,
  attributeFilters: Record<string, string>,
  cursor?: LogCursor,
) {
  const params: (string | number | Date)[] = [];
  const whereClauses: string[] = [];

  // Filter 1: By service name
  if (query.service !== undefined) {
    params.push(query.service);
    whereClauses.push(`service = $${params.length}`);
  }

  // Filter 2: By log level (debug, info, warn, error)
  if (query.level !== undefined) {
    params.push(query.level);
    whereClauses.push(`level = $${params.length}`);
  }

  // Filter 3: Start time (since)
  if (query.since !== undefined) {
    params.push(new Date(query.since));
    whereClauses.push(`timestamp >= $${params.length}`);
  }

  // Filter 4: End time (until)
  if (query.until !== undefined) {
    params.push(new Date(query.until));
    whereClauses.push(`timestamp < $${params.length}`);
  }

  // Filter 5: Search keyword inside the message
  if (query.q !== undefined) {
    params.push(`%${query.q}%`);
    whereClauses.push(`message ILIKE $${params.length}`);
  }

  // Filter 6: Pagination cursor (fetch items after this point in time/ID)
  if (cursor !== undefined) {
    params.push(new Date(cursor.timestamp), cursor.id);
    whereClauses.push(`(timestamp, id) < ($${params.length - 1}, $${params.length})`);
  }

  // Filter 7: Custom JSON attribute filters (e.g., attr.user_id = 123)
  const attrEntries = Object.entries(attributeFilters);
  for (const [key, value] of attrEntries) {
    params.push(key, value);
    whereClauses.push(`attributes->>$${params.length - 1} = $${params.length}`);
  }

  // Fetch 1 extra item to check if there is a next page
  params.push(query.limit + 1);
  const limitParam = `$${params.length}`;

  const whereSql = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
  const hasGinFilter = query.q !== undefined || attrEntries.length > 0;

  // Build the final SQL query (ordered newest first)
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

  // Execute query on read pool
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

  // Convert database rows into clean JSON response format
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

/**
 * DATA FLOW: Groups and counts logs into time buckets (1m, 5m, 1h, 1d).
 * Step 1: Determine the bucket size in minutes/hours/days.
 * Step 2: Apply time range and optional service/level filters.
 * Step 3: Run database aggregation query to count logs in each bucket.
 * Step 4: Return list of buckets with their counts.
 */
export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
): Promise<AggregateBucketResult[]> {
  const hasAttrFilters = Object.keys(attributeFilters).length > 0;

  // Fast path: Simple queries can be counted directly from database index
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
