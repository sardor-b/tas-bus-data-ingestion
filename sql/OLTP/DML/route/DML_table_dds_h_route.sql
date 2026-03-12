-- dds.h_route
INSERT INTO dds.h_route(hk_route, route_id, load_dt, load_source)
SELECT
    MD5((src.value ->> 'id')::TEXT) AS hk_route,

    (src.value ->> 'id')::INT AS route_id,

    NOW() AS load_dt,
    'stg.bus_routes' AS load_source
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
WHERE (src.value ->> 'id')::INT != 0
ON CONFLICT (hk_route)
    DO NOTHING
;