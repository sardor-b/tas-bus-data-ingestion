-- Spatial index for fast proximity searches
CREATE INDEX idx_stations_location ON dds.stations USING GIST (location);
-- B-Tree index for the Delta Load lookups
CREATE INDEX idx_stations_lookup ON dds.stations (station_id, create_dt DESC);