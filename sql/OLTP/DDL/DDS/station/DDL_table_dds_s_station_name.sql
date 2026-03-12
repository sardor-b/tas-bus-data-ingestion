create table dds.s_station_name (
    hk_station char(32) references dds.h_station(hk_station),

    name_UZ text not null,
    name_RU text not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_station, load_dt)
);