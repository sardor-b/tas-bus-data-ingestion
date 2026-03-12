-- dds.l_route_bus
insert into dds.l_route_bus
with src as (
    select
        jsonb_array_elements(object_value) as v
    from stg.bus_gps_updates
),
route_bus_combinations as (
    select
        v #>> '{bus, routeId}' as route_id,
        v #>> '{bus, id}' as bus_id
    from src
),
route_bus as (
    select distinct
        route_id,
        bus_id
    from route_bus_combinations
)
select
    md5(
        concat_ws(
         '||',
        route_id,
        bus_id
        )
    ) as hk_route_bus,

    md5(route_id) as hk_route,
    md5(bus_id) as hk_bus,

    now() as load_dt,
    'stg.bus_gps_updates' as load_source
from route_bus
on conflict (hk_route_bus)
    do nothing
;