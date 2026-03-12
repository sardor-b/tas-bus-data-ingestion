-- dds.s_station_location
INSERT INTO dds.s_station_location (hk_station, location, load_dt, load_source, hash_diff)
WITH src AS (
    SELECT JSONB_ARRAY_ELEMENTS((object_value #>> '{gpsStations}')::JSONB) AS v
    FROM stg.bus_stations
    UNION ALL
    SELECT JSONB_ARRAY_ELEMENTS((object_value #>> '{gpsSecondStations}')::JSONB) AS v
    FROM stg.bus_stations
),

stations AS (
    SELECT DISTINCT ON ((v ->> 'stationId')::INT)
        (v ->> 'stationId')::TEXT AS station_id,
        ST_SetSRID(
            ST_MakePoint((v ->> 'lx')::FLOAT, (v ->> 'ly')::FLOAT),
            4326
        ) AS location
    FROM src
    ORDER BY (v ->> 'stationId')::INT
),

latest AS (
    SELECT DISTINCT ON (hk_station)
        hk_station,
        hash_diff
    FROM dds.s_station_location
    ORDER BY hk_station, load_dt DESC
)

SELECT
    MD5(station_id)      AS hk_station,
    location,
    NOW()                AS load_dt,
    'stg.bus_stations'   AS load_source,
    MD5(ST_AsText(location)) AS hash_diff
FROM stations
LEFT JOIN latest l ON l.hk_station = MD5(station_id)
WHERE MD5(ST_AsText(location)) IS DISTINCT FROM l.hash_diff
;