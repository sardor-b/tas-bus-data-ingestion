create table dds.s_route_schedule (
    hk_route char(32) references dds.h_route(hk_route),

    start_time time not null,
    end_time time not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_route, load_dt)
);