FROM python:3.12-slim

WORKDIR /app

# System dependency needed by psycopg2 (the Postgres driver) to compile/
# link correctly on the slim base image, which omits build tools by
# default to keep the image small.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements.txt first, before the rest of the code. Docker
# caches each layer; as long as requirements.txt does not change, this
# pip install layer is reused on every rebuild instead of re-running,
# which makes iterating on pipeline code much faster to rebuild.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY sql/ ./sql/

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "shopsphere_pipelines.run_pipeline"]
