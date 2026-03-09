create table dds.route_fleet (
    id uuid primary key default uuidv7(),
    route_id uuid references dds.route_id_name(id),
    bus_type varchar not null,
    fleet_size int not null,
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);

