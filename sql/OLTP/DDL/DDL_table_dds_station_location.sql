create table dds.station_location (
    id uuid primary key default uuidv7(),
    station_id uuid references dds.station_id_name(id),
    location geometry(Point, 4326),
    direction varchar not null
);