from kafka import KafkaConsumer
import json
# Listen to Dead Letter Queue (DLQ) topic
consumer = KafkaConsumer(
    "orders-dlq",
    bootstrap_servers="localhost:9092",
    group_id="dlq-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Listening to DLQ...")

for msg in consumer:
    print("DLQ Message:", msg.value)
