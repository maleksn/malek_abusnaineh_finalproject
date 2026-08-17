CREATE EXTENSION IF NOT EXISTS pg_trgm;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_message_trgm_idx" ON "logs" USING gin ("message" gin_trgm_ops) WITH (fastupdate = on, gin_pending_list_limit = 4096);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_timestamp_service_level_idx" ON "logs" ("timestamp", "service", "level");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_attributes_gin_idx" ON "logs" USING gin ("attributes" jsonb_path_ops);
