create table dds.routes_path
(
    route_id         uuid                       not null references dds.routes(id),
    origin_path      GEOMETRY(LineString, 4326) not null,
    destination_path GEOMETRY(LineString, 4326) not null,
    create_dt        timestamptz                not null default now()
);