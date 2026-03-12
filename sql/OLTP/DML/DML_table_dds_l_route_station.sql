-- dds.l_route_station
INSERT INTO dds.l_route_station (hk_route_station, hk_route, hk_station, load_dt, load_source)
WITH src AS (
    SELECT
        object_value #>> '{route, id}' as route_id,
        jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
    FROM stg.bus_stations
    WHERE (object_value #>> '{route, id}')::int != 0
    UNION ALL
    SELECT
        object_value #>> '{route, id}' as route_id,
        jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
    FROM stg.bus_stations
    WHERE (object_value #>> '{route, id}')::int != 0
),
route_stations AS (
    SELECT DISTINCT
        route_id,
        (v ->> 'stationId') as station_id
    FROM src
)
SELECT
    md5(
        concat_ws(
         '||',
        route_id,
        station_id
        )
    ) as hk_route_station,

    md5(route_id) as hk_route,
    md5(station_id) as hk_station,

    now() as load_dt,
    'stg.bus_stations' as load_source
FROM route_stations
ON CONFLICT (hk_route_station)
    DO NOTHING
;
