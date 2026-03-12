CREATE UNLOGGED TABLE stg.bus_gps_updates (
    id           uuid        DEFAULT uuidv7(),
    source       varchar     NOT NULL,
    object_value jsonb       NOT NULL,
    update_dt    timestamptz NOT NULL DEFAULT now()
);
