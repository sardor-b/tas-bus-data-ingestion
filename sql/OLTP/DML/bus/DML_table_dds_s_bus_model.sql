-- dds.s_bus_model
INSERT INTO dds.s_bus_model (hk_bus, model, load_dt, load_source, hash_diff)
WITH latest AS (
    SELECT DISTINCT ON (hk_bus)
        hk_bus,
        hash_diff
    FROM dds.s_bus_model
    ORDER BY hk_bus, load_dt DESC
)
SELECT
    md5(src.bus_hash_id) AS hk_bus,
    src.bus_model,
    NOW()                AS load_dt,
    'stg.bus_gps_updates' AS load_source,
    md5(src.bus_model)   AS hash_diff
FROM (
    SELECT DISTINCT ON (v #>> '{bus, id}')
        v #>> '{bus, id}' AS bus_hash_id,
        v #>> '{bus, innerType}' AS bus_model
    FROM stg.bus_gps_updates,
    LATERAL jsonb_array_elements(object_value) AS v
) src
LEFT JOIN latest l ON l.hk_bus = md5(src.bus_hash_id)
WHERE md5(src.bus_model) IS DISTINCT FROM l.hash_diff;