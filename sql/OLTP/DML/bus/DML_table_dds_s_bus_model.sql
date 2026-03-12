-- dds.s_bus_model
INSERT INTO dds.s_bus_model (hk_bus, model, load_dt, load_source, hash_diff)
WITH latest AS (
    SELECT
        hk_bus,
        hash_diff
    FROM dds.s_bus_model
    ORDER BY hk_bus
)
SELECT
    md5(src.bus_hash_id) as hk_bus,

    src.bus_model,

    NOW() AS load_dt,
    'stg.bus_gps_updates' as load_source,
    md5(
        concat_ws(
            '||',
            src.bus_hash_id,
            src.bus_model
        )
    ) AS hash_diff
FROM (
    SELECT DISTINCT ON (v #>> '{bus, id}')
        v #>> '{bus, id}' AS bus_hash_id,
        v #>> '{bus, innerType}' AS bus_model
    FROM
        stg.bus_gps_updates,
    LATERAL jsonb_array_elements(object_value) AS v
) src
LEFT JOIN latest l on l.hk_bus = md5(src.bus_hash_id)
WHERE
    md5(
        concat_ws(
            '||',
            src.bus_hash_id,
            src.bus_model
        )
    ) IS DISTINCT FROM l.hash_diff
;