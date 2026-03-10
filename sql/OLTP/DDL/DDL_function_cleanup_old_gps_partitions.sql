CREATE OR REPLACE FUNCTION stg.cleanup_old_gps_partitions()
RETURNS void AS $$
DECLARE
    _retention_cutoff timestamptz;
    _partition_record record;
BEGIN
    -- We want to keep the current hour, so we set the cutoff
    -- to the beginning of the current hour.
    _retention_cutoff := date_trunc('hour', now());

    FOR _partition_record IN
        SELECT
            nmsp_child.nspname  AS child_schema,
            child.relname       AS child_name
        FROM pg_inherits
            JOIN pg_class parent        ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child         ON pg_inherits.inhrelid   = child.oid
            JOIN pg_namespace nmsp_child  ON child.relnamespace     = nmsp_child.oid
            JOIN pg_namespace nmsp_parent ON parent.relnamespace    = nmsp_parent.oid
        WHERE nmsp_parent.nspname = 'stg'
          AND parent.relname = 'bus_gps_updates'
    LOOP
        -- Extract timestamp from the name 'bus_gps_updates_YYYYMMDD_HH24'
        -- This logic assumes your naming convention remains consistent.
        BEGIN
            IF to_timestamp(substring(_partition_record.child_name from '\d{8}_\d{2}'), 'YYYYMMDD_HH24') < _retention_cutoff THEN
                RAISE NOTICE 'Dropping old partition: %', _partition_record.child_name;
                EXECUTE format('DROP TABLE IF EXISTS %I.%I',
                    _partition_record.child_schema,
                    _partition_record.child_name
                );
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Skip tables that don't match the timestamp pattern
            RAISE WARNING 'Skipping table %: Name does not match partition pattern.', _partition_record.child_name;
        END;
    END LOOP;
END;
$$ LANGUAGE plpgsql;