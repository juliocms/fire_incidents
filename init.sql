CREATE TABLE IF NOT EXISTS fire_incidents (
    incident_number VARCHAR(50),
    incident_date TIMESTAMP,
    incident_time TIME,
    battalion VARCHAR(50),
    district VARCHAR(100),
    neighborhood VARCHAR(100),
    incident_type VARCHAR(100),
    incident_description TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
); 