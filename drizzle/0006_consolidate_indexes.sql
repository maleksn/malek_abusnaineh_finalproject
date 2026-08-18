CREATE INDEX IF NOT EXISTS "logs_timestamp_id_service_level_idx" ON "logs" USING btree ("timestamp" ASC, "id" ASC, "service", "level");--> statement-breakpoint
DROP INDEX IF EXISTS "logs_timestamp_id_idx";--> statement-breakpoint
DROP INDEX IF EXISTS "logs_timestamp_service_level_idx";
