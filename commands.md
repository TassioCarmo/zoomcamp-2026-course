docker run -it \
    --rm \
    -v $(pwd)/test:/app/test \
    --entrypoint=bash \
    python:3.9.16-slim

   docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5433:5432 \
  postgres:18


  Install pgcli:

uv add --dev pgcli

Now use it to connect to Postgres:

uv run pgcli -h localhost -p 5432 -u root -d ny_taxi