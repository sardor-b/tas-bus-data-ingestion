insert into dds.bus_movement (bus_hash_id, route_id, location, speed, course, status, direction, ping_dt)
with bus_gps_updates as (
    select
        update_dt,
        jsonb_array_elements(object_value) as v
    from stg.bus_gps_updates
    where
        update_dt::date = current_date
)
select
    v #>> '{bus, id}' as bus_hash_id,
    (v #>> '{bus, routeId}')::int as route_id,
    st_setsrid(
        st_makepoint(
            (v #>> '{bus, ly}')::float,
            (v #>> '{bus, lx}')::float
        ), 4326
    ) as location, -- location ping
    (v #>> '{bus, speed}')::numeric(5,1) as speed,
    (v #>> '{course}')::int as course, -- degree to which bus is headed
    v #>> '{status}' as status, -- NOTINROUTE,INPARK,LOSTTIME,LATENCY,ONLINE
    case
        when (v #>> '{qDirection}')::int = 1 then 'origin'
        when (v #>> '{qDirection}')::int = 0 then 'destination'
        when (v #>> '{qDirection}')::int = -1 then 'None'
    end as direction,
    update_dt
from bus_gps_updates
;

create table dds.bus_movement (
    movement_id uuid primary key default uuidv7(),
    bus_hash_id varchar(32),
    route_id int,
    location geometry(Point, 4326),
    speed numeric(5,1),
    course int,
    status varchar,
    direction varchar,
    ping_dt timestamptz,
    create_dt timestamptz default now()
);


select (current_date)::date

select count(*) from dds.bus_movement;

select pg_size_pretty(pg_total_relation_size('dds.bus_movement'));
select
    max()
from dds.bus_movement

with bus_gps_updates as (
    select
        update_dt,
        jsonb_array_elements(object_value) as v
    from stg.bus_gps_updates
    where
        update_dt::date = current_date
)
select
    to_timestamp((v #>> '{bus, apiCreatedDate}')::double precision/1000.0),
    update_dt,
    ((update_dt)-to_timestamp((v #>> '{bus, apiCreatedDate}')::double precision/1000.0))::time
from bus_gps_updates
order by
    ((update_dt)-to_timestamp((v #>> '{bus, apiCreatedDate}')::double precision/1000.0))::time asc;

select to_timestamp(1773178452000/1000.0)

select pg_size_pretty(pg_total_relation_size('stg.bus_gps_updates'))
union
select pg_size_pretty(pg_total_relation_size('dds.bus_movement'))

select count(*) from dds.bus_movement;