-- dds.s_station_name
INSERT INTO dds.s_station_name (hk_station, name_uz, name_ru, load_dt, load_source, hash_diff)
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
    FROM dds.s_station_name
    ORDER BY hk_station
),
stations AS (
    SELECT DISTINCT ON ((v ->> 'stationId')::int)
        (v ->> 'stationId')::text as station_id,
        v ->> 'uzName' as name_uz,
        v ->> 'name' as name_ru
    FROM src
)
SELECT
    md5(station_id) as hk_station,

    name_uz,
    name_ru,

    NOW() AS load_dt,
    'stg.bus_stations' AS load_source,
    MD5(
        CONCAT_WS(
            '||',
            station_id,
            name_uz,
            name_ru
        )
    )
FROM stations
LEFT JOIN latest l ON l.hk_station = md5(station_id)
WHERE
    MD5(
        CONCAT_WS(
           '||',
            station_id,
            name_uz,
            name_ru
        )
    ) IS DISTINCT FROM l.hash_diff
;