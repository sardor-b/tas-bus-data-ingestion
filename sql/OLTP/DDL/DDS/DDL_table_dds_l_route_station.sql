create table dds.l_route_station (
    hk_route_station char(32) primary key,
    hk_route char(32) references dds.h_route(hk_route),
    hk_station char(32) references dds.h_station(hk_station),
    load_dt timestamptz default now(),
    load_source varchar(50) not null
);

create index idx_l_rs_route on dds.l_route_station(hk_route);
create index idx_l_rs_station on dds.l_route_station(hk_station);