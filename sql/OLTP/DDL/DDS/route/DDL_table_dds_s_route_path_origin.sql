create table dds.s_route_path_origin (
    hk_route char(32) references dds.h_route(hk_route),

    path geometry(LineString, 4326),

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_route, load_dt)
);
