#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to start..."
sleep 30

# Create topics
docker exec kafka kafka-topics --create \
  --topic events_raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --topic events_deadletter \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Topics created successfully"