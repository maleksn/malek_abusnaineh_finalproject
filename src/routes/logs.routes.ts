import { Router } from "express";
import {
  logSchema,
  logsRequestSchema,
  logsQuerySchema,
  aggregateQuerySchema,
} from "../validators/logs.validator";
import type { ValidLog } from "../validators/logs.validator";
import {
  enqueueLogs,
  queryLogs,
  aggregateLogs,
} from "../services/logs.service";
import { extractAttributeFilters } from "../utils/extractAttributeFilters";
import { encodeCursor, decodeCursor } from "../utils/cursor";

type RejectedLog = {
  index: number;
  reason: string;
};

const logsRouter = Router();

logsRouter.post("/", async (req, res) => {
  const requestResult = logsRequestSchema.safeParse(req.body);

  if (!requestResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
    });
  }

  const logs = requestResult.data.logs;
  const accepted: ValidLog[] = [];
  const rejected: RejectedLog[] = [];

  logs.forEach((log, index) => {
    const result = logSchema.safeParse(log);

    if (result.success) {
      accepted.push(result.data);
    } else {
      const reason = result.error.issues
        .map((issue) => issue.message)
        .join(" & ");

      rejected.push({
        index,
        reason,
      });
    }
  });

  if (accepted.length === 0) {
    return res.status(400).json({
      accepted: 0,
      rejected,
    });
  }

  await enqueueLogs(accepted);

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

export default logsRouter;