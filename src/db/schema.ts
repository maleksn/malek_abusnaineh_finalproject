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
    // Consolidated primary covering index for timestamp sorting, pagination, and fast aggregates
    index("logs_timestamp_id_service_level_idx").on(
      table.timestamp.asc(),
      table.id.asc(),
      table.service,
      table.level,
    ),
    // Filter by service with timestamp sorting
    index("logs_service_timestamp_id_idx").on(
      table.service,
      table.timestamp.desc(),
      table.id.desc(),
    ),
  ],
);
