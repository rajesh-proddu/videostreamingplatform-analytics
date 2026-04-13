FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

ENV KAFKA_BROKERS=localhost:9092
ENV KAFKA_VIDEO_TOPIC=video-events
ENV KAFKA_GROUP_ID=kafka-es-consumer
ENV ELASTICSEARCH_URL=http://localhost:9200
ENV ES_VIDEO_INDEX=videos

USER nobody

CMD ["python", "-m", "src.consumer"]
