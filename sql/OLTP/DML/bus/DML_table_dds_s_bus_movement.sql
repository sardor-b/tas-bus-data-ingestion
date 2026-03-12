-- dds.s_bus_movement
insert into dds.s_bus_movement (hk_bus, location, speed, course, status, direction, ping_dt, load_dt, load_source, hash_diff)
with bus_gps_updates as (
    select
        update_dt,
        jsonb_array_elements(object_value) as v
    from stg.bus_gps_updates
),
latest AS (
    SELECT
        hk_bus,
        hash_diff
    FROM dds.s_bus_garage_number
    ORDER BY hk_bus
),
bus_movement as (
    select
        v #>> '{bus, id}' as bus_id,
        st_setsrid(
            st_makepoint(
                (v #>> '{bus, ly}')::float,
                (v #>> '{bus, lx}')::float
            ), 4326
        ) as location, -- location ping
        (v #>> '{bus, speed}') as speed,
        (v #>> '{course}') as course,
        v #>> '{status}' as status,
        case
            when (v #>> '{qDirection}')::int = 1 then 'origin'
            when (v #>> '{qDirection}')::int = 0 then 'destination'
            when (v #>> '{qDirection}')::int = -1 then 'None'
        end as direction,
        update_dt as ping_dt
    from bus_gps_updates
)
select
    md5(bus_id) as hk_bus,

    location,
    speed::numeric(5,1),
    course::int,
    status,
    direction,
    ping_dt,

    now() as load_dt,
    'stg.bus_gps_updates' as load_source,
    md5(
        concat_ws(
            '||',
            st_asbinary(location),
            speed,
            course,
            status,
            direction,
            ping_dt
        )
    ) as hash_diff
from bus_movement
left join latest l on l.hk_bus = md5(bus_id)
where
    md5(
        concat_ws(
            '||',
            st_asbinary(location),
            speed,
            course,
            status,
            direction,
            ping_dt
        )
    ) is distinct from l.hash_diff
;
