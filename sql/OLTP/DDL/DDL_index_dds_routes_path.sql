-- Index the foreign key for faster joins
CREATE INDEX idx_routes_path_route_id ON dds.routes_path (route_id);

-- Spatial indexes for path-based queries
CREATE INDEX idx_routes_origin_path ON dds.routes_path USING GIST (origin_path);
CREATE INDEX idx_routes_dest_path ON dds.routes_path USING GIST (destination_path);