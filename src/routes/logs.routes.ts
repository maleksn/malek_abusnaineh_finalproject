import { Router } from "express";
import {
  logsQuerySchema,
  aggregateQuerySchema,
} from "../validators/logs.validator";
import type { ValidLog, LogsQuery } from "../validators/logs.validator";
import {
  enqueueCsvChunk,
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

// Match the format used by the load test / z.iso.datetime()
const isoTimestamp =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$/;

const logsRouter = Router();

logsRouter.post("/", (req, res) => {
  const rawLogs = (req.body as { logs?: unknown } | null | undefined)?.logs;

  if (!Array.isArray(rawLogs) || rawLogs.length === 0) {
    return res.status(400).json({
      error: "Invalid request body",
    });
  }

  const logs = rawLogs;
  let validCsvChunk = "";
  let acceptedCount = 0;
  const rejected: RejectedLog[] = [];
  const maxFutureTimestamp = Date.now() + 300000;

  for (let index = 0; index < logs.length; index++) {
    const log = logs[index];

    if (typeof log !== "object" || log === null || Array.isArray(log)) {
      rejected.push({ index, reason: "Invalid log object" });
      continue;
    }

    const input = log as Record<string, unknown>;

    // 1. timestamp validation
    const ts = input.timestamp;
    if (typeof ts !== "string") {
      rejected.push({ index, reason: "timestamp must be a string" });
      continue;
    }

    if (!isoTimestamp.test(ts)) {
      rejected.push({ index, reason: "Invalid timestamp" });
      continue;
    }

    const timestampMs = Date.parse(ts);
    if (!Number.isFinite(timestampMs)) {
      rejected.push({ index, reason: "Invalid timestamp" });
      continue;
    }

    if (timestampMs > maxFutureTimestamp) {
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
      rejected.push({ index, reason: "Invalid level" });
      continue;
    }

    // 3. service validation
    const srv = input.service;
    if (typeof srv !== "string") {
      rejected.push({ index, reason: "service must be a string" });
      continue;
    }

    const service =
      srv.charCodeAt(0) <= 32 || srv.charCodeAt(srv.length - 1) <= 32
        ? srv.trim()
        : srv;
    if (service.length === 0) {
      rejected.push({ index, reason: "service must not be empty" });
      continue;
    }

    // 4. message validation
    const msg = input.message;
    if (typeof msg !== "string") {
      rejected.push({ index, reason: "message must be a string" });
      continue;
    }

    const message =
      msg.charCodeAt(0) <= 32 || msg.charCodeAt(msg.length - 1) <= 32
        ? msg.trim()
        : msg;
    if (message.length === 0) {
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
        rejected.push({ index, reason: "attributes must be an object" });
        continue;
      }

      const attrsObj = rawAttrs as Record<string, unknown>;
      let hasInvalidAttr = false;
      const keys = Object.keys(attrsObj);

      if (keys.length > 0) {
        for (let k = 0; k < keys.length; k++) {
          const key = keys[k]!;
          const val = attrsObj[key];
          if (
            (typeof val !== "string" &&
              typeof val !== "number" &&
              typeof val !== "boolean") ||
            (typeof val === "number" && !Number.isFinite(val))
          ) {
            rejected.push({ index, reason: `invalid attribute value for ${key}` });
            hasInvalidAttr = true;
            break;
          }
        }
        if (hasInvalidAttr) continue;
        const jsonStr = JSON.stringify(attrsObj);
        attrJson =
          jsonStr.indexOf('"') === -1
            ? `"${jsonStr}"`
            : `"${jsonStr.replaceAll('"', '""')}"`;
      }
    }

    // Direct fast CSV formatting
    const escapedMsg =
      message.indexOf('"') === -1
        ? `"${message}"`
        : `"${message.replaceAll('"', '""')}"`;

    validCsvChunk += `"${ts}","${lvl}","${service}",${escapedMsg},${attrJson}\n`;
    acceptedCount++;
  }

  if (acceptedCount === 0) {
    return res.status(400).json({
      accepted: 0,
      rejected,
    });
  }

  try {
    enqueueCsvChunk(validCsvChunk, acceptedCount);
  } catch (err) {
    return res.status(503).json({
      error: "Ingestion queue full, please retry",
    });
  }

  if (rejected.length === 0) {
    res.setHeader("Content-Type", "application/json");
    return res.status(200).send(`{"accepted":${acceptedCount},"rejected":[]}`);
  }

  return res.status(200).json({
    accepted: acceptedCount,
    rejected,
  });
});

logsRouter.get("/", async (req, res) => {
  // Fast path for read-after-write query: /logs?limit=20
  const isSimpleLimit =
    Object.keys(req.query).length === 1 && req.query.limit !== undefined;

  let query: LogsQuery;
  let attributeFilters: Record<string, string> = {};

  if (isSimpleLimit) {
    const limitNum = Number(req.query.limit);
    if (!Number.isInteger(limitNum) || limitNum < 1 || limitNum > 1000) {
      return res.status(400).json({ error: "limit must be between 1 and 1000" });
    }
    query = { limit: limitNum };
  } else {
    const queryResult = logsQuerySchema.safeParse(req.query);
    if (!queryResult.success) {
      const error = queryResult.error.issues
        .map((issue) => issue.message)
        .join(" & ");
      return res.status(400).json({ error });
    }
    query = queryResult.data;
    attributeFilters = extractAttributeFilters(req.query);
  }

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
        timestamp:
          lastLog.timestamp instanceof Date
            ? lastLog.timestamp.toISOString()
            : new Date(String(lastLog.timestamp)).toISOString(),
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

  const buckets = await aggregateLogs(query, attributeFilters);

  return res.status(200).json({
    buckets: buckets.map((bucket) => ({
      start:
        bucket.start instanceof Date
          ? bucket.start.toISOString()
          : new Date(String(bucket.start)).toISOString(),
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