CREATE INDEX idx_bus_gps_source
    ON stg.bus_gps_updates (source, update_dt DESC);

CREATE INDEX idx_bus_gps_payload
    ON stg.bus_gps_updates USING GIN (object_value jsonb_path_ops);
