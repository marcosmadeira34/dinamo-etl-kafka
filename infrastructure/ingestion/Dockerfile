# infrastructure/ingestion/Dockerfile
# Imagem leve, SEM Spark - dinamo-ingestion e um Deployment HTTP comum.
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

COPY infrastructure/ingestion/requirements-ingestion.txt /tmp/requirements-ingestion.txt
RUN pip install --no-cache-dir --default-timeout=300 -r /tmp/requirements-ingestion.txt

COPY src/apps/ingestion_app.py /opt/app/apps/ingestion_app.py
COPY src/config /opt/app/src/config
COPY src/kafka /opt/app/src/kafka
COPY src/ingestion /opt/app/src/ingestion
COPY src/observability /opt/app/src/observability
COPY src/connectors /opt/app/src/connectors           

ENV PYTHONPATH="/opt/app/src"
EXPOSE 8080

CMD ["python", "/opt/app/apps/ingestion_app.py"]

