# San Francisco Fire Incidents Analysis

This project implements a data warehouse solution for analyzing fire incident data in the city of San Francisco.

## Requirements

- Docker and Docker Compose
- Git (optional)

## Setup and Execution

1. **Clone the repository** (optional):
   ```bash
   git clone [repository-url]
   cd [repository-name]
   ```

3. **Configure environment variables**:
   - The `.env` file is already configured with default values
   - You can modify it if needed

4. **Start the services**:
   ```bash
   docker-compose up -d
   ```

   This will start:
   - PostgreSQL on port 5432
   - pgAdmin on port 5050
   - ETL service (which will wait for PostgreSQL to be ready)

6. **Access pgAdmin**:
   - Open: http://localhost:5050
   - Login:
     - Email: admin@admin.com
     - Password: admin
   - Add a new server:
     - Host: postgres
     - Port: 5432
     - Database: sf_fire_incidents
     - Username: postgres
     - Password: postgres

7. **Run reports**:
   - Use pgAdmin's Query Tool
   - Paste the content of `scripts/reports/incident_analysis.sql`
   - Execute the query

## Monitoring

- ETL process logs can be viewed with:
  ```bash
  docker-compose logs -f etl
  ```

- Container status:
  ```bash
  docker-compose ps
  ```

## Data Quality

The project includes automated tests to ensure data quality:

- Required fields validation
- Duplicate detection
- Date format validation
- Coordinate range validation
- District validation

## Troubleshooting

If you encounter issues:

1. Check container logs:
   ```bash
   docker-compose logs
   ```

2. Verify database connection:
   ```bash
   docker-compose exec postgres psql -U postgres -d sf_fire_incidents
   ```

3. Check file permissions:
   ```bash
   ls -l data/fire_incidents.csv
   ```