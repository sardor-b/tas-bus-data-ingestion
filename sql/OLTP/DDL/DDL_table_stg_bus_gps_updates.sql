create unlogged table stg.bus_gps_updates (
    id uuid primary key default uuidv7(),
    source varchar not null,
    object_value jsonb not null,
    update_dt timestamptz default now()
);
