create table dds.h_station (
    hk_station char(32) primary key,
    station_id int not null,
    load_dt timestamptz default now(),
    load_source varchar(50) not null
);