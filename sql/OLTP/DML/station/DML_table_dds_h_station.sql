-- dds.h_station
INSERT INTO dds.h_station (hk_station, station_id, load_dt, load_source)
WITH src AS (
    SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
    FROM stg.bus_stations
    UNION
    SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
    FROM stg.bus_stations
),
stations AS (
    SELECT DISTINCT ON ((v ->> 'stationId')::int)
        (v ->> 'stationId')::int as station_id
    FROM src
)
SELECT
    md5(station_id::text) as hk_station,

    station_id,

    NOW() AS load_dt,
    'stg.bus_stations' AS load_source
FROM stations
ON CONFLICT (hk_station)
    DO NOTHING
;
