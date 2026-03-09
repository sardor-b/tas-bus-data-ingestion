create table dds.route_id_name (
    id uuid primary key default uuidv7(),
    route_id int unique not null,
    route_name varchar not null,
    update_dt timestamptz default now(),
    create_dt timestamptz default now()
);
