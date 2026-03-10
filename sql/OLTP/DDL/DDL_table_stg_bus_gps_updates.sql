-- ============================================================
-- PARTITIONED TABLE
-- ============================================================
CREATE TABLE stg.bus_gps_updates (
    id           uuid        NOT NULL DEFAULT uuidv7(),
    source       varchar     NOT NULL,
    object_value jsonb       NOT NULL,
    update_dt    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, update_dt)
) PARTITION BY RANGE (update_dt);


-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX idx_bus_gps_source
    ON stg.bus_gps_updates (source, update_dt DESC);

CREATE INDEX idx_bus_gps_payload
    ON stg.bus_gps_updates USING GIN (object_value jsonb_path_ops);


-- ============================================================
-- PARTITION MANAGEMENT via pg_partman
-- ============================================================
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

SELECT partman.create_parent(
    p_parent_table  => 'stg.bus_gps_updates',
    p_control       => 'update_dt',
    p_interval      => '15 minutes',
    p_premake       => 4
);

-- Apply storage params to the partman template table --
-- pg_partman copies these to every new partition it creates
ALTER TABLE partman.template_stg_bus_gps_updates SET (
    fillfactor                      = 70,
    autovacuum_enabled              = true,
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.01
);

-- Retention
UPDATE partman.part_config
SET
    retention                = '1 hour',
    retention_keep_table     = false,
    retention_keep_index     = false,
    infinite_time_partitions = true
WHERE parent_table = 'stg.bus_gps_updates';


-- ============================================================
-- PARTMAN MAINTENANCE JOB (pg_cron)
-- ============================================================
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule(
    'partman-maintain-bus-gps',
    '*/15 * * * *',
    $$CALL partman.run_maintenance_proc()$$
);