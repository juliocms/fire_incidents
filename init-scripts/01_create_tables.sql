-- Tabela de incidentes de incêndio
CREATE TABLE IF NOT EXISTS fire_incidents (
    incident_number VARCHAR(50) PRIMARY KEY,
    incident_date TIMESTAMP,
    incident_time TIME,
    district VARCHAR(100),
    battalion VARCHAR(50),
    neighborhood VARCHAR(100),
    incident_type VARCHAR(100),
    incident_description TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Função para atualizar o timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger para atualizar o timestamp
CREATE TRIGGER update_fire_incidents_updated_at
    BEFORE UPDATE ON fire_incidents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column(); 