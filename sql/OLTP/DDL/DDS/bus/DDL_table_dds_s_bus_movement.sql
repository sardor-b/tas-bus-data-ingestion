create table dds.s_bus_movement (
    hk_bus char(32) references dds.h_bus(hk_bus),

    location geometry(Point, 4326) not null,
    speed numeric(5,1) not null,
    course int not null not null,
    status varchar(12) not null,
    direction varchar(12) not null,
    ping_dt timestamptz not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_bus, ping_dt)
);
