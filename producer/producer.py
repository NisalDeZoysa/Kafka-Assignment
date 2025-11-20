from kafka import KafkaProducer
from fastavro import schemaless_writer
import io
import json
import random
import time
import uuid

# Load Avro schema
schema = json.loads(open("../avro/order.avsc").read())

def avro_serialize(record):
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, record)
    return bytes_writer.getvalue()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=avro_serialize
)

products = ["Laptop", "Keyboard", "Mouse", "Headphones", "Monitor"]

while True:
    record = {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(products),
        "price": round(random.uniform(50, 500), 2)
    }

    producer.send("orders", value=record)
    print("Sent:", record)

    time.sleep(1)
