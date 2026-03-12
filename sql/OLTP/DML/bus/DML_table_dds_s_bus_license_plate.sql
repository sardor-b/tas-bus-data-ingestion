-- dds.s_bus_license_plate
INSERT INTO dds.s_bus_license_plate (hk_bus, license_plate, load_dt, load_source, hash_diff)
WITH latest AS (
    SELECT
        hk_bus,
        hash_diff
    FROM dds.s_bus_license_plate
    ORDER BY hk_bus, load_dt DESC
)
SELECT
    md5(src.bus_hash_id) as hk_bus,

    src.license_plate,

    NOW() AS load_dt,
    'stg.bus_gps_updates' as load_source,
    md5(src.license_plate) AS hash_diff
FROM (
    SELECT DISTINCT ON (v #>> '{bus, id}')
        v #>> '{bus, id}' AS bus_hash_id,
        v #>> '{bus, gos}' AS license_plate
    FROM
        stg.bus_gps_updates,
    LATERAL jsonb_array_elements(object_value) AS v
) src
LEFT JOIN latest l on l.hk_bus = md5(src.bus_hash_id)
WHERE md5(src.license_plate) IS DISTINCT FROM l.hash_diff
;
