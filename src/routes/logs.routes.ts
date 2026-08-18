import { Router } from "express";
import {
  logsQuerySchema,
  aggregateQuerySchema,
} from "../validators/logs.validator";
import type { ValidLog } from "../validators/logs.validator";
import {
  enqueueLogs,
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

logsRouter.post("/", async (req, res) => {
  const rawLogs = (req.body as { logs?: unknown } | null | undefined)?.logs;

  if (!Array.isArray(rawLogs) || rawLogs.length === 0) {
    return res.status(400).json({
      error: "Invalid request body",
    });
  }

  const logs = rawLogs;
  const accepted: ValidLog[] = [];
  const rejected: RejectedLog[] = [];

  function validateLogManually(
    log: unknown,
  ): { valid: true; data: ValidLog } | { valid: false; reason: string } {
    if (typeof log !== "object" || log === null || Array.isArray(log)) {
      return { valid: false, reason: "Invalid log object" };
    }

    const input = log as Record<string, unknown>;

    // timestamp
    if (typeof input.timestamp !== "string") {
      return { valid: false, reason: "timestamp must be a string" };
    }



    if (!isoTimestamp.test(input.timestamp)) {
      return { valid: false, reason: "Invalid timestamp" };
    }

    const timestampMs = Date.parse(input.timestamp);

    if (!Number.isFinite(timestampMs)) {
      return { valid: false, reason: "Invalid timestamp" };
    }

    if (timestampMs > Date.now() + 300000) {
      return {
        valid: false,
        reason: "timestamp must not be more than five minutes in the future",
      };
    }

    // level
    if (
      input.level !== "debug" &&
      input.level !== "info" &&
      input.level !== "warn" &&
      input.level !== "error"
    ) {
      return {
        valid: false,
        reason: "Invalid level",
      };
    }

    // service
    if (typeof input.service !== "string") {
      return {
        valid: false,
        reason: "service must be a string",
      };
    }

    const service = input.service.trim();

    if (service.length === 0) {
      return {
        valid: false,
        reason: "service must not be empty",
      };
    }

    // message
    if (typeof input.message !== "string") {
      return {
        valid: false,
        reason: "message must be a string",
      };
    }

    const message = input.message.trim();

    if (message.length === 0) {
      return {
        valid: false,
        reason: "message must not be empty",
      };
    }

    // attributes
    let attributes: Record<string, string | number | boolean>;

    if (input.attributes === undefined) {
      attributes = {};
    } else {
      if (
        typeof input.attributes !== "object" ||
        input.attributes === null ||
        Array.isArray(input.attributes)
      ) {
        return {
          valid: false,
          reason: "attributes must be an object",
        };
      }

      const rawAttributes = input.attributes as Record<string, unknown>;
      attributes = {};

      for (const [key, value] of Object.entries(rawAttributes)) {
        if (
          typeof value !== "string" &&
          typeof value !== "number" &&
          typeof value !== "boolean"
        ) {
          return {
            valid: false,
            reason: `invalid attribute value for ${key}`,
          };
        }

        if (typeof value === "number" && !Number.isFinite(value)) {
          return {
            valid: false,
            reason: `invalid attribute value for ${key}`,
          };
        }

        attributes[key] = value;
      }
    }

    return {
      valid: true,
      data: {
        timestamp: input.timestamp,
        level: input.level,
        service,
        message,
        attributes,
      },
    };
  }

  for (let index = 0; index < logs.length; index++) {
    const log = logs[index];

    const result = validateLogManually(log);

    if (result.valid) {
      accepted.push(result.data);
    } else {
      rejected.push({
        index,
        reason: result.reason,
      });
    }
  }

  if (accepted.length === 0) {
    return res.status(400).json({
      accepted: 0,
      rejected,
    });
  }
  try {
    enqueueLogs(accepted);
  } catch (err) {
    return res.status(503).json({
      error: "Ingestion queue full, please retry",
    });
  }
  return res.status(200).json({
    accepted: accepted.length,
    rejected,
  });
});

logsRouter.get("/", async (req, res) => {
  const queryResult = logsQuerySchema.safeParse(req.query);

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

  const lastLog = page[page.length - 1]!;

  const nextCursor = hasMore
    ? encodeCursor({
      timestamp: lastLog.timestamp.toISOString(),
      id: lastLog.id,
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
      start: new Date(String(bucket.start)).toISOString(),
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