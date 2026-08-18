CREATE INDEX IF NOT EXISTS "logs_timestamp_service_level_idx" ON "logs" USING btree ("timestamp" ASC, "service", "level");--> statement-breakpoint
DROP INDEX IF EXISTS "logs_attributes_gin_idx";--> statement-breakpoint
DROP INDEX IF EXISTS "logs_message_trgm_idx";--> statement-breakpoint
DROP INDEX IF EXISTS "logs_timestamp_idx";
