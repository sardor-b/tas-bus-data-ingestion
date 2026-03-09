upd_w: 101639 bytes
upd_f: 5 seconds
upd_t: 24 hours
route_count: 170

17280 * 170 = 2937600 requests/day

Overall: 2937600*101639 = 298574726400 bytes = 298.57 GB/day

for 1H: 10800*101639 = 1097701200 bytes = 1.098 GB/hour

ingest raw JSON to stg -> remove after 1H

------------------------------------------------------------

amn0 dependencies:
- dagster webserver: http://172.29.172.1:3001/
- postgresql 172.29.172.1:5432
- clickhouse: 172.29.172.1:8123
- kafka ui: http://172.29.172.1:8080/
- grafana: http://172.29.172.1:3000/

stg.bus_routes -> dds.buses -> [info about buses] -> get updates about bus stations & gps updates


