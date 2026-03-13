create table dds.s_route_number (
    hk_route char(32) references dds.h_route(hk_route),

    number varchar (10) not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_route, load_dt)
);