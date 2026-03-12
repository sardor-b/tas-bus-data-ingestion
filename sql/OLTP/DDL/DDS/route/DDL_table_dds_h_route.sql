create table dds.h_route (
    hk_route char(32) primary key,
    route_id int not null,
    load_dt timestamptz default now(),
    load_source varchar(50) not null
);
