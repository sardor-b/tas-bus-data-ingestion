-- dds.s_route_number
INSERT INTO dds.s_route_number (hk_route, number, load_dt, load_source, hash_diff)
SELECT
    MD5((src.value ->> 'id')::TEXT) AS hk_route,

    (src.value ->> 'name') AS number,

    NOW() AS load_dt,
    'stg.bus_routes' AS load_source,
    MD5(src.value ->> 'name') AS hash_diff
FROM (
    SELECT
        JSONB_ARRAY_ELEMENTS(v.object_value) AS value
    FROM (
        SELECT object_value
        FROM stg.bus_routes
        ORDER BY update_dt DESC
        LIMIT 1
    ) AS v
) AS src
LEFT JOIN (
    SELECT DISTINCT ON (hk_route)
        hk_route,
        hash_diff
    FROM dds.s_route_number
    ORDER BY hk_route, load_dt DESC
) latest ON latest.hk_route = MD5((src.value ->> 'id')::TEXT)
WHERE
    (src.value ->> 'id')::INT != 0
    AND MD5(src.value ->> 'name') IS DISTINCT FROM latest.hash_diff
;