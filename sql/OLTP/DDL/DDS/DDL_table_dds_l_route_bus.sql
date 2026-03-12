create table dds.l_route_bus (
    hk_route_bus char(32) primary key,
    hk_route char(32) references dds.h_route(hk_route),
    hk_bus char(32) references dds.h_bus(hk_bus),
    load_dt timestamptz default now(),
    load_source varchar(50) not null
);

create index idx_l_rb_route on dds.l_route_bus(hk_route);
create index idx_l_rb_bus on dds.l_route_bus(hk_bus);