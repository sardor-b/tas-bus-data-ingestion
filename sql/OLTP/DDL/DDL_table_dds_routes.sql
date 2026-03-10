create table dds.routes (
    id UUID primary key default uuidv7(),
    route_id INT not null,
    route_name VARCHAR not null,
    bus_type VARCHAR not null,
    fleet_size INTEGER not null,
    start_time TIME not null,
    end_time TIME not null,
    origin_name_uz VARCHAR not null,
    destination_name_uz VARCHAR not null,
    origin_name_ru VARCHAR not null,
    destination_name_ru VARCHAR not null,
    create_dt TIMESTAMPTZ default now()
);