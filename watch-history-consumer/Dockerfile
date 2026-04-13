FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

ENV KAFKA_BROKERS=localhost:9092
ENV KAFKA_WATCH_TOPIC=watch-events
ENV KAFKA_GROUP_ID=watch-history-consumer
ENV ICEBERG_CATALOG_NAME=nessie
ENV ICEBERG_CATALOG_TYPE=rest
ENV ICEBERG_CATALOG_URI=http://localhost:19120/iceberg/
ENV ICEBERG_WAREHOUSE=s3://iceberg-warehouse/
ENV S3_ENDPOINT=http://localhost:9000
ENV ICEBERG_TABLE=analytics.watch_history

USER nobody

CMD ["python", "-m", "src.consumer"]
