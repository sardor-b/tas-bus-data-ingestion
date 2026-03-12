-- dds.s_route_name
INSERT INTO dds.s_route_name (hk_route, origin_name_uz, origin_name_ru, destination_name_uz, destination_name_ru, load_dt, load_source, hash_diff)
SELECT
    MD5((src.value ->> 'id')::TEXT) AS hk_route,

    (src.value ->> 'uzNameA') AS origin_name_uz,
    (src.value ->> 'nameA')   AS origin_name_ru,
    (src.value ->> 'uzNameB') AS destination_name_uz,
    (src.value ->> 'nameB')   AS destination_name_ru,

    NOW() AS load_dt,
    'stg.bus_routes' AS load_source,
    MD5(
        CONCAT_WS(
            '||',
            (src.value ->> 'uzNameA'),
            (src.value ->> 'nameA'),
            (src.value ->> 'uzNameB'),
            (src.value ->> 'nameB')
        )
    ) AS hash_diff
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
    FROM dds.s_route_name
    ORDER BY hk_route, load_dt DESC
) latest ON latest.hk_route = MD5((src.value ->> 'id')::TEXT)
WHERE
    (src.value ->> 'id')::INT != 0
    AND MD5(
        CONCAT_WS(
            '||',
            (src.value ->> 'uzNameA'),
            (src.value ->> 'nameA'),
            (src.value ->> 'uzNameB'),
            (src.value ->> 'nameB')
        )
    ) IS DISTINCT FROM latest.hash_diff
;