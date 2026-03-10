CREATE TABLE stg.bus_gps_updates (
    id           uuid        NOT NULL DEFAULT uuidv7(),
    source       varchar     NOT NULL,
    object_value jsonb       NOT NULL,
    update_dt    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, update_dt)
) PARTITION BY RANGE (update_dt);