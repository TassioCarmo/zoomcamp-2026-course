# Use a valid stable version (3.13-slim or 3.12-slim)
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# Ensure the virtualenv bin is in the PATH
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first to leverage Docker caching
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

# Copy the script
COPY ingest_data.py .

# Entrypoint to run the script
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]