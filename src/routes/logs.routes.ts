import { Router } from "express";
import { pool, readPool } from "../db";
import {
  logsQuerySchema,
  aggregateQuerySchema,
} from "../validators/logs.validator";
import type { ValidLog, LogsQuery } from "../validators/logs.validator";
import {
  enqueueCsvChunk,
  flushCurrentBuffer,
  queryLogs,
  aggregateLogs,
} from "../services/logs.service";
import {
  purgeExpiredLogs,
  getRetentionStatus,
} from "../services/retention.service";
import { extractAttributeFilters } from "../utils/extractAttributeFilters";
import { encodeCursor, decodeCursor } from "../utils/cursor";

type RejectedLog = {
  index: number;
  reason: string;
};

function isValidIsoTimestamp(ts: string): boolean {
  const len = ts.length;
  if (len < 19 || len > 35) return false;
  if (ts.charCodeAt(4) !== 45 || ts.charCodeAt(7) !== 45) return false; // '-'
  if (ts.charCodeAt(10) !== 84) return false; // 'T'
  if (ts.charCodeAt(13) !== 58 || ts.charCodeAt(16) !== 58) return false; // ':'
  const parsed = Date.parse(ts);
  return !Number.isNaN(parsed);
}

let cachedMaxFutureMs = Date.now() + 300000;
setInterval(() => {
  cachedMaxFutureMs = Date.now() + 300000;
}, 1000);

const logsRouter = Router();

logsRouter.post("/", async (req, res) => {
  const rawLogs = (req.body as { logs?: unknown } | null | undefined)?.logs;

  if (!Array.isArray(rawLogs) || rawLogs.length === 0) {
    return res.status(400).json({
      error: "Invalid request body",
    });
  }

  const logs = rawLogs;
  const len = logs.length;
  const lines: string[] = new Array(len);
  let acceptedCount = 0;
  let rejected: RejectedLog[] | null = null;
  const maxFutureMs = cachedMaxFutureMs;

  for (let index = 0; index < len; index++) {
    const log = logs[index];

    if (typeof log !== "object" || log === null || Array.isArray(log)) {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "Invalid log object" });
      continue;
    }

    const input = log as Record<string, unknown>;

    // 1. timestamp validation
    const ts = input.timestamp;
    if (typeof ts !== "string") {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "timestamp must be a string" });
      continue;
    }

    if (!isValidIsoTimestamp(ts)) {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "Invalid timestamp" });
      continue;
    }

    const parsedMs = Date.parse(ts);
    if (parsedMs > maxFutureMs) {
      if (!rejected) rejected = [];
      rejected.push({
        index,
        reason: "timestamp must not be more than five minutes in the future",
      });
      continue;
    }

    // 2. level validation
    const lvl = input.level;
    if (
      lvl !== "debug" &&
      lvl !== "info" &&
      lvl !== "warn" &&
      lvl !== "error"
    ) {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "Invalid level" });
      continue;
    }

    // 3. service validation
    const srv = input.service;
    if (typeof srv !== "string") {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "service must be a string" });
      continue;
    }

    const service =
      srv.charCodeAt(0) <= 32 || srv.charCodeAt(srv.length - 1) <= 32
        ? srv.trim()
        : srv;
    if (service.length === 0) {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "service must not be empty" });
      continue;
    }

    // 4. message validation
    const msg = input.message;
    if (typeof msg !== "string") {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "message must be a string" });
      continue;
    }

    const message =
      msg.charCodeAt(0) <= 32 || msg.charCodeAt(msg.length - 1) <= 32
        ? msg.trim()
        : msg;
    if (message.length === 0) {
      if (!rejected) rejected = [];
      rejected.push({ index, reason: "message must not be empty" });
      continue;
    }

    // 5. attributes validation & direct CSV serialization
    let attrJson = '"{}"';
    const rawAttrs = input.attributes;
    if (rawAttrs !== undefined) {
      if (
        typeof rawAttrs !== "object" ||
        rawAttrs === null ||
        Array.isArray(rawAttrs)
      ) {
        if (!rejected) rejected = [];
        rejected.push({ index, reason: "attributes must be an object" });
        continue;
      }

      const attrsObj = rawAttrs as Record<string, unknown>;
      let hasInvalidAttr = false;

      for (const key in attrsObj) {
        const val = attrsObj[key];
        if (
          (typeof val !== "string" &&
            typeof val !== "number" &&
            typeof val !== "boolean") ||
          (typeof val === "number" && !Number.isFinite(val))
        ) {
          if (!rejected) rejected = [];
          rejected.push({ index, reason: `invalid attribute value for ${key}` });
          hasInvalidAttr = true;
          break;
        }
      }
      if (hasInvalidAttr) continue;
      const jsonStr = JSON.stringify(attrsObj);
      attrJson = `"${jsonStr.replace(/"/g, '""')}"`;
    }

    // Direct fast CSV formatting
    const escapedService =
      service.indexOf('"') === -1
        ? `"${service}"`
        : `"${service.replace(/"/g, '""')}"`;

    const escapedMsg =
      message.indexOf('"') === -1
        ? `"${message}"`
        : `"${message.replace(/"/g, '""')}"`;

    lines[acceptedCount++] = `"${ts}","${lvl}",${escapedService},${escapedMsg},${attrJson}`;
  }

  // Free body reference immediately
  (req as unknown as { body: unknown }).body = null;

  if (acceptedCount === 0) {
    return res.status(400).json({
      accepted: 0,
      rejected: rejected ?? [],
    });
  }

  lines.length = acceptedCount;
  const validCsvChunk = lines.join("\n") + "\n";

  await enqueueCsvChunk(validCsvChunk);

  if (!rejected || rejected.length === 0) {
    res.setHeader("Content-Type", "application/json");
    return res.status(200).send(`{"accepted":${acceptedCount},"rejected":[]}`);
  }

  return res.status(200).json({
    accepted: acceptedCount,
    rejected,
  });
});

logsRouter.get("/", async (req, res) => {
  const queryResult = logsQuerySchema.safeParse(req.query);
  if (!queryResult.success) {
    const error = queryResult.error.issues
      .map((issue) => issue.message)
      .join(" & ");
    return res.status(400).json({ error });
  }
  const query = queryResult.data;
  const attributeFilters = extractAttributeFilters(req.query);

  let cursor;
  if (query.cursor !== undefined) {
    try {
      cursor = decodeCursor(query.cursor);
    } catch {
      return res.status(400).json({
        error: "Invalid cursor",
      });
    }
  }

  const logs = await queryLogs(query, attributeFilters, cursor);

  const hasMore = logs.length > query.limit;
  const page = hasMore ? logs.slice(0, query.limit) : logs;

  const lastLog = page[page.length - 1];

  const nextCursor =
    hasMore && lastLog
      ? encodeCursor({
        timestamp: lastLog.timestamp,
        id: Number(lastLog.id),
      })
      : null;

  return res.status(200).json({
    logs: page,
    next_cursor: nextCursor,
  });
});

logsRouter.get("/aggregate", async (req, res) => {
  const queryResult = aggregateQuerySchema.safeParse(req.query);

  if (!queryResult.success) {
    const error = queryResult.error.issues
      .map((issue) => issue.message)
      .join(" & ");

    return res.status(400).json({
      error,
    });
  }

  const query = queryResult.data;
  const attributeFilters = extractAttributeFilters(req.query);
  const hasAttrFilters = Object.keys(attributeFilters).length > 0;

  // Ultra-fast zero-allocation path for Index-Only aggregation
  if (!hasAttrFilters && query.q === undefined) {
    try {
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

      const sqlResult = await readPool.query<{
        start: Date | string;
        group: string | null;
        count: number;
      }>({
        name: queryName,
        text: sqlText,
        values: params,
      });

      return res.status(200).json({
        buckets: sqlResult.rows.map((row) => ({
          start:
            typeof row.start === "string"
              ? row.start
              : (row.start instanceof Date ? row.start.toISOString() : String(row.start)),
          group: row.group,
          count: row.count,
        })),
      });
    } catch (err) {
      console.error("GET /logs/aggregate error:", err);
      return res.status(500).json({ error: "Aggregation query failed" });
    }
  }

  const buckets = await aggregateLogs(query, attributeFilters);

  return res.status(200).json({
    buckets: buckets.map((bucket) => ({
      start:
        typeof bucket.start === "string"
          ? bucket.start
          : (bucket.start instanceof Date ? bucket.start.toISOString() : String(bucket.start)),
      group: bucket.group,
      count: bucket.count,
    })),
  });
});

// retention routes
logsRouter.get("/retention/status", (_req, res) => {
  return res.status(200).json(getRetentionStatus());
});

logsRouter.post("/retention/cleanup", async (req, res) => {
  const days = req.body?.days !== undefined ? Number(req.body.days) : undefined;
  const batchSize = req.body?.batch_size !== undefined ? Number(req.body.batch_size) : undefined;
  const result = await purgeExpiredLogs(days, batchSize);
  return res.status(200).json({
    success: true,
    deleted_count: result.deletedCount,
    duration_ms: result.durationMs,
  });
});

export default logsRouter;