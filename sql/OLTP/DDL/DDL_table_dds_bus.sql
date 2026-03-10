CREATE TABLE dds.bus (
    id uuid primary key default uuidv7(),
    bus_hash_id VARCHAR(32),             -- The hex string e.g., "31660a0b010000000000998939816325"
    bus_id INT NOT NULL,                 -- Internal integer ID e.g., 583
    garage_number VARCHAR(20),           -- e.g., "5-260"
    license_plate VARCHAR(20),           -- e.g., "01 308 PKA"
    bus_type VARCHAR(50),                -- innerType e.g., "Yutong CNG"
    create_dt TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
