import { z } from "zod";

// for POST /logs request validation
const MAX_FUTURE_OFFSET_MS = 300000; // 5 minutes in milliseconds

const attributeValueSchema = z.union([z.string(), z.number(), z.boolean()]);

const attributesSchema = z.record(z.string(), attributeValueSchema);

export const logSchema = z.object({
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

  level: z.enum(["debug", "info", "warn", "error"]),

  service: z.string().trim().min(1),

  message: z.string().trim().min(1),

  attributes: attributesSchema.default({}),
});

export type ValidLog = z.infer<typeof logSchema>;

export const logsRequestSchema = z.object({
  logs: z.array(z.unknown()).min(1),
});

// for GET /logs query parameters validation
const queryTimestampSchema = z.iso.datetime();

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

// for GET /logs/aggregate query parameters validation
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
  .refine(
    (query) => {
      return new Date(query.until).getTime() >= new Date(query.since).getTime();
    },
    {
      message: "until must not be earlier than since",
      path: ["until"],
    },
  );

  // this is just to make zod is the source of truth for the types, so we can use them in the codebase without having to manually define them
export type AggregateQuery = z.infer<typeof aggregateQuerySchema>;

export type LogsQuery = z.infer<typeof logsQuerySchema>;
