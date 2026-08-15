import type {
  ValidLog,
  LogsQuery,
  AggregateQuery,
} from "../validators/logs.validator";
import { db, pool } from "../db";
import { logs as logsTable } from "../db/schema";
import type { LogCursor } from "../utils/cursor";

import { from as copyFrom } from "pg-copy-streams"; // to use COPY instead of INSERT

import { and, desc, eq, gte, lt, sql } from "drizzle-orm";

// ==========================================
// Batch Ingest Aggregator
// ==========================================
const FLUSH_MAX_LOGS = 5000; // حجم الدفعة الكبيرة
const FLUSH_WAIT_MS = 20; // أقصى تأخير قبل الفلاش
const MAX_PENDING_LOGS = 50000; // حد أمان للذاكرة
const COPY_CHUNK_SIZE = 65536; // 64 KB

type PendingInsert = {
  logs: ValidLog[];
  resolve: () => void;
  reject: (error: unknown) => void;
};

let pendingInserts: PendingInsert[] = [];
let pendingLogCount = 0;
let flushTimer: ReturnType<typeof setTimeout> | null = null;
let flushing = false;

function backpressureError(): Error {
  const error = new Error("ingestion queue is full");
  error.name = "BackpressureError";
  return error;
}

function clearFlushTimer() {
  if (flushTimer !== null) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
}

function scheduleFlushIfNeeded() {
  if (pendingInserts.length === 0) return;

  if (pendingLogCount >= FLUSH_MAX_LOGS) {
    clearFlushTimer();
    void flushPendingInserts();
  } else if (flushTimer === null) {
    flushTimer = setTimeout(() => {
      flushTimer = null;
      void flushPendingInserts();
    }, FLUSH_WAIT_MS);
  }
}

async function flushPendingInserts() {
  if (flushing || pendingInserts.length === 0) return;

  flushing = true;
  clearFlushTimer();

  const batch = pendingInserts;
  pendingInserts = [];
  pendingLogCount = 0;

  const logs = batch.flatMap((entry) => entry.logs);

  try {
    await insertLogsBulk(logs);

    for (const entry of batch) {
      entry.resolve();
    }
  } catch (error) {
    for (const entry of batch) {
      entry.reject(error);
    }
  } finally {
    flushing = false;
    scheduleFlushIfNeeded();
  }
}

function csvField(value: string): string {
  if (value.indexOf('"') === -1) {
    return `"${value}"`;
  }

  return `"${value.replaceAll('"', '""')}"`;
}

/**
 * إدخال دفعة كبيرة بواسطة UNNEST
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

/**
 * إضافة سجلات إلى قائمة الانتظار المجمّعة
 */
export function enqueueLogs(logs: ValidLog[]): Promise<void> {
  if (pendingLogCount + logs.length > MAX_PENDING_LOGS) {
    return Promise.reject(backpressureError());
  }

  return new Promise((resolve, reject) => {
    pendingInserts.push({ logs, resolve, reject });
    pendingLogCount += logs.length;
    scheduleFlushIfNeeded();
  });
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
    conditions.push(
      sql`LOWER(${logsTable.message}) LIKE LOWER(${"%" + query.q + "%"})`,
    );
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

function getBucketSeconds(bucket: AggregateQuery["bucket"]): number {
  switch (bucket) {
    case "1m":
      return 60;

    case "5m":
      return 300;

    case "1h":
      return 3600;

    case "1d":
      return 86400;
  }
}

export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
) {
  const bucketSeconds = getBucketSeconds(query.bucket);

  const bucketExpression = sql<Date>`
    to_timestamp(
      floor(
        extract(epoch from ${logsTable.timestamp})
        / ${bucketSeconds}
      ) * ${bucketSeconds}
    )
  `;

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
    conditions.push(
      sql`LOWER(${logsTable.message}) LIKE LOWER(${"%" + query.q + "%"})`,
    );
  }

  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} ->> ${key} = ${value}`);
  }

  return db
    .select({
      start: bucketExpression,
      group: groupExpression,
      count: sql<number>`count(*)::int`,
    })
    .from(logsTable)
    .where(and(...conditions))
    .groupBy(sql.raw("1"), sql.raw("2"))
    .orderBy(sql.raw("1"));
}
