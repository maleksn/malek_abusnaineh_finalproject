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
    // Range index for lightning-fast aggregate queries
    index("logs_timestamp_idx").on(table.timestamp.asc()),
    // Filter by service with timestamp sorting
    index("logs_service_timestamp_id_idx").on(
      table.service,
      table.timestamp.desc(),
      table.id.desc(),
    ),
  ],
);
