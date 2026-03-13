INSERT INTO dds.s_bus_garage_number (hk_bus, garage_number, load_dt, load_source, hash_diff)
WITH latest AS (
    SELECT DISTINCT ON (hk_bus)
        hk_bus,
        hash_diff
    FROM dds.s_bus_garage_number
    ORDER BY hk_bus, load_dt DESC
)
SELECT
    md5(src.bus_hash_id)   AS hk_bus,
    src.garage_number,
    NOW()                  AS load_dt,
    'stg.bus_gps_updates'  AS load_source,
    md5(src.garage_number) AS hash_diff
FROM (
    SELECT DISTINCT ON (v #>> '{bus, id}')
        v #>> '{bus, id}' AS bus_hash_id,
        v #>> '{bus, gar}' AS garage_number
    FROM stg.bus_gps_updates,
    LATERAL jsonb_array_elements(object_value) AS v
) src
LEFT JOIN latest l ON l.hk_bus = md5(src.bus_hash_id)
WHERE md5(src.garage_number) IS DISTINCT FROM l.hash_diff
ON CONFLICT (hk_bus, load_dt)
    DO NOTHING;