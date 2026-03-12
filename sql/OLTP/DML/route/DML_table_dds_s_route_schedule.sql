-- dds.s_route_schedule
INSERT INTO dds.s_route_schedule (hk_route, start_time, end_time, load_dt, load_source, hash_diff)
SELECT
    MD5((src.value ->> 'id')::TEXT) AS hk_route,

    (src.value ->> 'startTime')::TIME AS start_time,
    (src.value ->> 'endTime')::TIME AS end_time,

    NOW() AS load_dt,
    'stg.bus_routes' AS load_source,
    MD5(
        CONCAT_WS(
            '||',
            (src.value ->> 'id')::TEXT,
            (src.value ->> 'startTime')::TIME::TEXT,
            (src.value ->> 'endTime')::TIME::TEXT
        )
    ) AS hash_diff
FROM (
    SELECT
        JSONB_ARRAY_ELEMENTS(v.object_value) AS value
    FROM
        (
        SELECT
            object_value
        FROM stg.bus_routes
        ORDER BY update_dt DESC
        LIMIT 1
        ) AS v
) AS src
LEFT JOIN (
    SELECT DISTINCT ON (hk_route)
        hk_route,
        hash_diff
    FROM dds.s_route_schedule
) latest ON latest.hk_route = MD5((src.value ->> 'id')::TEXT)
WHERE
    (src.value ->> 'id')::INT != 0
    AND MD5(
        CONCAT_WS(
            '||',
            (src.value ->> 'id')::TEXT,
            (src.value ->> 'startTime')::TIME::TEXT,
            (src.value ->> 'endTime')::TIME::TEXT
        )
    ) IS DISTINCT FROM latest.hash_diff
;