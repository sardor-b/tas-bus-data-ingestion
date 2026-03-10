-- Schedule to run at the start of every hour
SELECT cron.schedule('0 * * * *', 'SELECT stg.cleanup_old_gps_partitions(); SELECT stg.generate_hourly_gps_partitions();');