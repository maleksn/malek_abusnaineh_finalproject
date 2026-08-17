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

export const logLevelEnum = pgEnum("log_level", [
  "debug",
  "info",
  "warn",
  "error",
]);

export const logs = pgTable(
  "logs",
  {
    id: bigserial("id", {
      mode: "number",
    }).primaryKey(),

    timestamp: timestamp("timestamp", {
      withTimezone: true,
    }).notNull(),

    level: logLevelEnum("level").notNull(),

    service: varchar("service", {
      length: 255,
    }).notNull(),

    message: text("message").notNull(),

    attributes: jsonb("attributes").notNull().default({}),

    createdAt: timestamp("created_at", {
      withTimezone: true,
    })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    // Primary index for default timestamp sorting and pagination
    index("logs_timestamp_id_idx").on(table.timestamp.desc(), table.id.desc()),
    // Filter by service with timestamp sorting
    index("logs_service_timestamp_id_idx").on(
      table.service,
      table.timestamp.desc(),
      table.id.desc(),
    ),
    // Composite index for fast filtering by service and level combined
    index("logs_service_level_ts_id_idx").on(
      table.service,
      table.level,
      table.timestamp.desc(),
      table.id.desc(),
    ),
    // Filter by level with timestamp sorting
    index("logs_level_ts_id_idx").on(
      table.level,
      table.timestamp.desc(),
      table.id.desc(),
    ),
    // Covering index for lightning-fast aggregate queries (Index-Only Scan)
    index("logs_timestamp_service_level_idx").on(
      table.timestamp,
      table.service,
      table.level,
    ),
    // GIN index for JSONB attribute equality queries
    index("logs_attributes_gin_idx").using(
      "gin",
      table.attributes.op("jsonb_path_ops"),
    ),
    // GIN index for case-insensitive substring message search
    index("logs_message_trgm_idx").using(
      "gin",
      table.message.op("gin_trgm_ops"),
    ),
  ],
);
