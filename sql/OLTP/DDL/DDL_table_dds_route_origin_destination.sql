create table dds.route_origin_destination (
    id uuid primary key default uuidv7(),
    route_id uuid references dds.route_id_name(id),
    origin_name_uz varchar not null,
    origin_name_ru varchar not null,
    destination_name_uz varchar not null,
    destination_name_ru varchar not null,
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);