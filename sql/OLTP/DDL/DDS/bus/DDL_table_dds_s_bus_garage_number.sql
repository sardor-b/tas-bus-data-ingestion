create table dds.s_bus_garage_number (
    hk_bus char(32) references dds.h_bus(hk_bus),

    garage_number varchar(12) not null,

    load_dt timestamptz default now(),
    load_source varchar(50) not null,
    hash_diff char(32) not null,

    primary key (hk_bus, load_dt)
);