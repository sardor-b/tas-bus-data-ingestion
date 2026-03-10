create table dds.routes (
    id UUID primary key default uuidv7(),
    route_id INT,
    route_name VARCHAR,
    origin_path GEOMETRY(LineString, 4326),
    destination_path GEOMETRY(LineString, 4326),
    bus_type VARCHAR,
    fleet_size INTEGER,
    start_time TIME,
    end_time TIME,
    origin_name_uz VARCHAR,
    destination_name_uz VARCHAR,
    origin_name_ru VARCHAR,
    destination_name_ru VARCHAR,
    create_dt TIMESTAMPTZ default now()
);