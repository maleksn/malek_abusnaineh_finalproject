CREATE EXTENSION IF NOT EXISTS pg_trgm;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_message_trgm_idx" ON "logs" USING gin ("message" gin_trgm_ops);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_timestamp_service_level_idx" ON "logs" ("timestamp", "service", "level");
