CREATE INDEX IF NOT EXISTS "logs_service_level_ts_id_idx" ON "logs" USING btree ("service","level","timestamp" DESC NULLS LAST,"id" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_level_ts_id_idx" ON "logs" USING btree ("level","timestamp" DESC NULLS LAST,"id" DESC NULLS LAST);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_timestamp_service_level_idx" ON "logs" USING btree ("timestamp","service","level");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_attributes_gin_idx" ON "logs" USING gin ("attributes" jsonb_path_ops);--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "logs_message_trgm_idx" ON "logs" USING gin ("message" gin_trgm_ops);
