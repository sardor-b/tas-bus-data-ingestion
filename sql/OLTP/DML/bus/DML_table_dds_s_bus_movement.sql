INSERT INTO dds.s_bus_movement (hk_bus, location, speed, course, status, direction, ping_dt, load_dt, load_source, hash_diff)
WITH bus_gps_updates AS (
    SELECT
        update_dt,
        jsonb_array_elements(object_value) AS v
    FROM stg.bus_gps_updates
),
latest AS (
    SELECT
        hk_bus,
        ping_dt
    FROM dds.s_bus_movement
),
bus_movement AS (
    SELECT DISTINCT ON (bus_id, ping_dt)
        v #>> '{bus, id}'      AS bus_id,
        st_setsrid(
            st_makepoint(
                (v #>> '{bus, ly}')::float,
                (v #>> '{bus, lx}')::float
            ), 4326
        )                      AS location,
        v #>> '{bus, speed}'   AS speed,
        v #>> '{course}'       AS course,
        v #>> '{status}'       AS status,
        CASE
            WHEN (v #>> '{qDirection}')::int =  1 THEN 'origin'
            WHEN (v #>> '{qDirection}')::int =  0 THEN 'destination'
            WHEN (v #>> '{qDirection}')::int = -1 THEN 'None'
        END                    AS direction,
        update_dt              AS ping_dt
    FROM bus_gps_updates
    ORDER BY bus_id, ping_dt
)
SELECT
    md5(bm.bus_id)            AS hk_bus,
    bm.location,
    bm.speed::numeric(5,1),
    bm.course::int,
    bm.status,
    bm.direction,
    bm.ping_dt,
    NOW()                  AS load_dt,
    'stg.bus_gps_updates'  AS load_source,
    md5(
        concat_ws(
            '||',
            st_astext(bm.location),
            bm.speed,
            bm.course,
            bm.status,
            bm.direction,
            bm.ping_dt
        )
    )                      AS hash_diff
FROM bus_movement bm
LEFT JOIN latest l
    ON l.hk_bus = md5(bus_id)
    AND l.ping_dt = bm.ping_dt
WHERE l.hk_bus IS NULL
;