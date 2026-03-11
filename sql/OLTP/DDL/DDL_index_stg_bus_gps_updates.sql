CREATE INDEX idx_bus_gps_updates_update_dt
    ON stg.bus_gps_updates (update_dt DESC);