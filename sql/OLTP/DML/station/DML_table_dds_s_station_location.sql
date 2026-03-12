-- dds.s_station_location
INSERT INTO dds.s_station_location (hk_station, location, load_dt, load_source, hash_diff)
WITH src AS (
    SELECT jsonb_array_elements((object_value #>> '{gpsStations}')::jsonb) as v
    FROM stg.bus_stations
    UNION
    SELECT jsonb_array_elements((object_value #>> '{gpsSecondStations}')::jsonb) as v
    FROM stg.bus_stations
),
latest AS (
    SELECT
        hk_station,
        hash_diff
    FROM dds.s_station_location
    ORDER BY hk_station
),
stations AS (
    SELECT DISTINCT ON ((v ->> 'stationId')::int)
        (v ->> 'stationId')::text as station_id,
        ST_SetSRID(ST_MakePoint((v ->> 'lx')::float, (v ->> 'ly')::float), 4326) as location
    FROM src
)
SELECT
    md5(station_id) as hk_station,

    location,

    NOW() AS load_dt,
    'stg.bus_stations' AS load_source,
    MD5(
        CONCAT_WS(
            '||',
            station_id,
            st_asbinary(location)
        )
    )
FROM stations
LEFT JOIN latest l ON l.hk_station = md5(station_id)
WHERE
    MD5(
        CONCAT_WS(
            '||',
            station_id,
            st_asbinary(location)
        )
    ) IS DISTINCT FROM l.hash_diff
;