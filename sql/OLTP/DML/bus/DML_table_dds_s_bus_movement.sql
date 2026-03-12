INSERT INTO dds.s_bus_movement (hk_bus, location, speed, course, status, direction, ping_dt, load_dt, load_source, hash_diff)
WITH bus_gps_updates AS (
    SELECT
        update_dt,
        jsonb_array_elements(object_value) AS v
    FROM stg.bus_gps_updates
),
latest AS (
    SELECT DISTINCT ON (hk_bus)
        hk_bus,
        hash_diff
    FROM dds.s_bus_movement
    ORDER BY hk_bus, load_dt DESC
),
bus_movement AS (
    SELECT DISTINCT ON (bus_id, ping_dt)
        v #>> '{bus, id}'  AS bus_id,
        st_setsrid(
            st_makepoint(
                (v #>> '{bus, ly}')::float,  -- longitude first
                (v #>> '{bus, lx}')::float   -- latitude second
            ), 4326
        )                  AS location,
        v #>> '{bus, speed}' AS speed,
        v #>> '{course}'     AS course,
        v #>> '{status}'     AS status,
        CASE
            WHEN (v #>> '{qDirection}')::int =  1 THEN 'origin'
            WHEN (v #>> '{qDirection}')::int =  0 THEN 'destination'
            WHEN (v #>> '{qDirection}')::int = -1 THEN 'None'
        END                  AS direction,
        update_dt            AS ping_dt
    FROM bus_gps_updates
)
SELECT
    md5(bus_id)          AS hk_bus,
    location,
    speed::numeric(5,1),
    course::int,
    status,
    direction,
    ping_dt,
    NOW()                AS load_dt,
    'stg.bus_gps_updates' AS load_source,
    md5(
        concat_ws(
            '||',
            st_astext(location),
            speed,
            course,
            status,
            direction,
            ping_dt
        )
    )                    AS hash_diff
FROM bus_movement
LEFT JOIN latest l ON l.hk_bus = md5(bus_id)
WHERE md5(
    concat_ws(
        '||',
        st_astext(location),
        speed,
        course,
        status,
        direction,
        ping_dt
    )
) IS DISTINCT FROM l.hash_diff;