create table dds.route_coordinates (
    id uuid primary key default uuidv7(),
    route_id uuid references dds.route_id_name(id),
    origin_path geometry(LineString, 4326),
    destination_path geometry(LineString, 4326),
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);