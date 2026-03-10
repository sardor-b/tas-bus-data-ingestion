CREATE OR REPLACE FUNCTION stg.generate_hourly_gps_partitions()
RETURNS void AS $$
DECLARE
    _start_time timestamptz;
    _end_time   timestamptz;
    _part_name  text;
BEGIN
    -- Create partitions for the next 24 hours
    FOR i IN 0..24 LOOP
        _start_time := date_trunc('hour', now() + (i || ' hours')::interval);
        _end_time   := _start_time + interval '1 hour';
        _part_name  := 'stg.bus_gps_updates_' || to_char(_start_time, 'YYYYMMDD_HH24');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF stg.bus_gps_updates
             FOR VALUES FROM (%L) TO (%L)',
            _part_name, _start_time, _end_time
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;