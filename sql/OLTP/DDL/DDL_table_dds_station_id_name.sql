create table dds.station_id_name (
    id uuid primary key default uuidv7(),
    station_id int not null unique,
    station_name_uz varchar not null,
    station_name_ru varchar not null,
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);
