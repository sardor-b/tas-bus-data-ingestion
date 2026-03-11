create table dds.bus_movement (
    movement_id uuid primary key default uuidv7(),
    bus_hash_id varchar(32),
    route_id int,
    location geometry(Point, 4326),
    speed numeric(5,1),
    course int,
    status varchar,
    direction varchar,
    ping_dt timestamptz,
    create_dt timestamptz default now()
);