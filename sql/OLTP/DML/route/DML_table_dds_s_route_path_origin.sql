-- dds.s_route_path_origin
INSERT INTO dds.s_route_path_origin (hk_route, path, load_dt, load_source, hash_diff)
WITH src AS (
    SELECT DISTINCT ON ((object_value #>> '{route, id}')::INT)
        (object_value #>> '{route, id}')::INT AS route_id,
        (object_value -> 'coordsTwo')::JSONB   AS path
    FROM stg.bus_stations
    WHERE (object_value #>> '{route, id}')::INT != 0
),

src_with_geo AS (
    SELECT
        route_id,
        MD5(route_id::TEXT) AS hk_route,
        ST_SetSRID(
            ST_MakeLine(
                ST_MakePoint((p.val->>'ly')::FLOAT, (p.val->>'lx')::FLOAT)
                ORDER BY p.ord
            ),
            4326
        ) AS path
    FROM src
    CROSS JOIN LATERAL (
        SELECT val, ord
        FROM jsonb_array_elements(src.path) WITH ORDINALITY AS p(val, ord)
    ) p
    GROUP BY route_id
),

src_hashed AS (
    SELECT
        hk_route,
        path,
        MD5(
            CONCAT_WS('||', route_id::TEXT, ST_AsBinary(path))
        ) AS hash_diff
    FROM src_with_geo
),

latest AS (
    SELECT
        hk_route,
        hash_diff
    FROM dds.s_route_path_origin
    ORDER BY hk_route
)

SELECT
    s.hk_route,
    s.path,
    NOW()               AS load_dt,
    'stg.bus_stations'  AS load_source,
    s.hash_diff
FROM src_hashed s
LEFT JOIN latest l ON l.hk_route = s.hk_route
WHERE s.hash_diff IS DISTINCT FROM l.hash_diff
;