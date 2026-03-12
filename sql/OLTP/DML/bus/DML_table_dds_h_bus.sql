-- dds.h_bus
INSERT INTO dds.h_bus (hk_bus, bus_id, load_dt, load_source)
SELECT
    md5(src.bus_hash_id) as hk_bus,

    bus_hash_id,

    now() as load_dt,
    'stg.bus_gps_updates' as load_source
FROM (
    SELECT DISTINCT ON (v #>> '{bus, id}')
        v #>> '{bus, id}' AS bus_hash_id
    FROM
        stg.bus_gps_updates,
    LATERAL jsonb_array_elements(object_value) AS v
) src
ON CONFLICT (hk_bus)
    DO NOTHING
;