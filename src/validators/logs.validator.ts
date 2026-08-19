import { z } from "zod";

// =========================================================================
// VALIDATION SCHEMAS: Rules that check if client data is correct and safe
// =========================================================================

// Maximum allowed time into the future for log timestamps (5 minutes)
const MAX_FUTURE_OFFSET_MS = 300000;

// Allowed values inside the custom attributes object (strings without null bytes, numbers, or booleans)
const attributeValueSchema = z.union([
  z.string().refine((val) => !val.includes("\0"), "attribute value must not contain null bytes"),
  z.number(),
  z.boolean(),
]);

// Custom attributes must be an object of key-value pairs
const attributesSchema = z.record(
  z.string().refine((key) => !key.includes("\0"), "attribute key must not contain null bytes"),
  attributeValueSchema,
);

// =========================================================================
// 1. INCOMING LOG VALIDATION (POST /logs)
// =========================================================================
// Checks a single log item to make sure all required fields are present and valid
export const logSchema = z.object({
  // Must be ISO format date, and not more than 5 minutes in the future
  timestamp: z.iso.datetime().refine(
    (value) => {
      const timestampMs = new Date(value).getTime();
      const maxAllowedTime = Date.now() + MAX_FUTURE_OFFSET_MS;
      return timestampMs <= maxAllowedTime;
    },
    {
      message: "timestamp must not be more than five minutes in the future",
    },
  ),

  // Must be one of 4 allowed levels
  level: z.enum(["debug", "info", "warn", "error"]),

  // Must be a non-empty service name without null characters
  service: z.string().trim().min(1).refine((s) => !s.includes("\0"), "service must not contain null bytes"),

  // Must be a non-empty log message text without null characters
  message: z.string().trim().min(1).refine((m) => !m.includes("\0"), "message must not contain null bytes"),

  // Optional custom attributes object
  attributes: attributesSchema.default({}),
});

export type ValidLog = z.infer<typeof logSchema>;

// The POST request body must contain an array of at least 1 log
export const logsRequestSchema = z.object({
  logs: z.array(z.unknown()).min(1),
});

// =========================================================================
// 2. QUERY PARAMETERS VALIDATION (GET /logs)
// =========================================================================
const queryTimestampSchema = z.iso.datetime();

// Limit must be an integer number between 1 and 1000 (default: 100)
const queryLimitSchema = z
  .string()
  .regex(/^\d+$/, "limit must be a number")
  .transform(Number)
  .refine(
    (value) => value >= 1 && value <= 1000,
    "limit must be between 1 and 1000",
  );

export const logsQuerySchema = z
  .object({
    service: z.string().min(1).optional(),
    level: z.enum(["debug", "info", "warn", "error"]).optional(),
    since: queryTimestampSchema.optional(),
    until: queryTimestampSchema.optional(),
    q: z.string().min(1).optional(),
    cursor: z.string().min(1).optional(),
    limit: queryLimitSchema.optional().default(100),
  })
  // Ensure "until" date is not earlier than "since" date
  .refine(
    (query) => {
      if (query.since === undefined || query.until === undefined) {
        return true;
      }
      return new Date(query.until).getTime() >= new Date(query.since).getTime();
    },
    {
      message: "until must not be earlier than since",
      path: ["until"],
    },
  );

// =========================================================================
// 3. AGGREGATION PARAMETERS VALIDATION (GET /logs/aggregate)
// =========================================================================
export const aggregateQuerySchema = z
  .object({
    since: queryTimestampSchema,
    until: queryTimestampSchema,
    bucket: z.enum(["1m", "5m", "1h", "1d"]),
    group_by: z.enum(["service", "level"]).optional(),
    service: z.string().min(1).optional(),
    level: z.enum(["debug", "info", "warn", "error"]).optional(),
    q: z.string().min(1).optional(),
  })
  // Ensure "until" date is not earlier than "since" date
  .refine(
    (query) => {
      return new Date(query.until).getTime() >= new Date(query.since).getTime();
    },
    {
      message: "until must not be earlier than since",
      path: ["until"],
    },
  );

// Export types derived from the schemas
export type AggregateQuery = z.infer<typeof aggregateQuerySchema>;
export type LogsQuery = z.infer<typeof logsQuerySchema>;
