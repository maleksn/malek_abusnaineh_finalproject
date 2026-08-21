import {
  pgTable,
  bigserial,
  timestamp,
  text,
  varchar,
  jsonb,
  index,
  pgEnum,
} from "drizzle-orm/pg-core";

// =========================================================================
// LOG LEVEL DEFINITION: Allowed severity categories
// =========================================================================
export const logLevelEnum = pgEnum("log_level", [
  "debug", // Detailed developer troubleshooting info
  "info",  // Normal system events and activity
  "warn",  // Warning events that are not yet errors
  "error", // Failures or error events
]);

// =========================================================================
// LOGS DATABASE TABLE: How log records are stored in PostgreSQL
// =========================================================================
export const logs = pgTable(
  "logs",
  {
    // Unique automatic ID for each log entry (1, 2, 3...)
    id: bigserial("id", {
      mode: "number",
    }).primaryKey(),

    // When the log event actually occurred (with timezone)
    timestamp: timestamp("timestamp", {
      withTimezone: true,
    }).notNull(),

    // Severity level: debug, info, warn, or error
    level: logLevelEnum("level").notNull(),

    // Name of the application or service that sent the log (e.g. "auth-service")
    service: varchar("service", {
      length: 255,
    }).notNull(),

    // Main log text message describing the event
    message: text("message").notNull(),

    // Optional flexible JSON metadata (e.g. {"user_id": 123, "ip": "1.2.3.4"})
    attributes: jsonb("attributes").notNull().default({}),

    // Exact date and time when this record was inserted into the database
    createdAt: timestamp("created_at", {
      withTimezone: true,
    })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    // DATABASE INDEX 1: Composite Covering Index for Aggregations, Time-Range Filters & Grouping
    index("logs_timestamp_service_level_idx").on(
      table.timestamp.asc(),
      table.service,
      table.level,
    ),
    // DATABASE INDEX 2: Allows super-fast search by specific service name ordered by time
    index("logs_service_timestamp_id_idx").on(
      table.service,
      table.timestamp.desc(),
      table.id.desc(),
    ),
  ],
);
