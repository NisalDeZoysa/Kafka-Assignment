from kafka import KafkaConsumer, KafkaProducer
from fastavro import schemaless_reader
import io
import json
import random
import time

# Load Avro schema
schema = json.loads(open("../avro/order.avsc").read())

def avro_deserialize(msg_bytes):
    bytes_reader = io.BytesIO(msg_bytes)
    return schemaless_reader(bytes_reader, schema)

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    group_id="order-consumer-group",
    auto_offset_reset="earliest"
)

producer_dlq = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

total_price = 0
count = 0

def process_message(msg):
    """Random temporary failure simulation"""
    if random.random() < 0.2:
        raise Exception("Temporary random failure")

    return True

for message in consumer:
    record = avro_deserialize(message.value)
    print("\nReceived:", record)

    retries = 3
    for attempt in range(retries):
        try:
            process_message(record)
            break
        except Exception as e:
            print(f"Error: {e}, retry {attempt+1}/{retries}")
            time.sleep(1)
    else:
        print("Sending to DLQ:", record)
        producer_dlq.send("orders-dlq", value=record)
        continue

    # Update running average
    total_price += record['price']
    count += 1
    avg = total_price / count

    print(f"Running Average Price: {avg:.2f}")
