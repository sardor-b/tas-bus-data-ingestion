create table dds.s_station_location (
    hk_station char(32) references dds.h_station(hk_station),

    location geometry(Point, 4326),

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_station, load_dt)
);