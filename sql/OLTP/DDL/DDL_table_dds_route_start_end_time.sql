create table dds.route_start_end_time (
    id uuid primary key default uuidv7(),
    route_id uuid references dds.route_id_name(id),
    start_time time not null,
    end_time time not null,
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);