create table dds.h_bus (
    hk_bus char(32) primary key,
    bus_id varchar(32) not null,
    load_dt timestamptz default now(),
    load_source varchar(50) not null
);
