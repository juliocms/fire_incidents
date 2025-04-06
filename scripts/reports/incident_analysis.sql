WITH incident_stats AS (
    SELECT
        district,
        battalion,
        COUNT(*) as total_incidents,
        COUNT(DISTINCT incident_type) as unique_incident_types,
        MIN(incident_date) as first_incident,
        MAX(incident_date) as last_incident
    FROM fire_incidents
    GROUP BY district, battalion
)

SELECT
    district,
    battalion,
    total_incidents,
    unique_incident_types,
    first_incident,
    last_incident,
    ROUND(total_incidents * 100.0 / SUM(total_incidents) OVER (), 2) as percentage_of_total
FROM incident_stats
ORDER BY total_incidents DESC;

SELECT
    DATE_TRUNC('month', incident_date) as month,
    COUNT(*) as total_incidents,
    COUNT(DISTINCT incident_type) as unique_incident_types
FROM fire_incidents
GROUP BY month
ORDER BY month;

WITH ranked_incidents AS (
    SELECT
        district,
        incident_type,
        COUNT(*) as incident_count,
        ROW_NUMBER() OVER (PARTITION BY district ORDER BY COUNT(*) DESC) as rank
    FROM fire_incidents
    GROUP BY district, incident_type
)

SELECT
    district,
    incident_type,
    incident_count
FROM ranked_incidents
WHERE rank <= 5
ORDER BY district, rank; 