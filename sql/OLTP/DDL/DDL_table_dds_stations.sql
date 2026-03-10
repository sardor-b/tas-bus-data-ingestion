create table dds.stations (
    id uuid primary key default uuidv7(),
    station_id int not null unique,
    station_name_uz varchar not null,
    station_name_ru varchar not null,
    location geometry(Point, 4326),
    direction varchar not null,
    create_dt timestamptz default now()
);