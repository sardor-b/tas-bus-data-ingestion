create table dds.s_route_name (
    hk_route char(32) references dds.h_route(hk_route),

    origin_name_UZ text not null,
    origin_name_RU text not null,
    destination_name_UZ text not null,
    destination_name_RU text not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_route, load_dt)
);