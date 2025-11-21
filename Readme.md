1.  Project Overview

This system processes Order messages in real time using Kafka.
Every order contains:

orderId — unique UUID

product — random item

price — random float

A Kafka Producer sends serialized Avro messages to the orders topic.

A Kafka Consumer reads these messages, performs:

✔️ Real-time Aggregation

A continuously updated running average:

new_avg = (previous_total + price) / message_count

✔️ Retry Logic

If message processing fails, the consumer retries 3 times before marking failure.

✔️ Dead Letter Queue (DLQ)

Messages that fail permanently are stored in the orders-dlq topic.

A DLQ Consumer reads and logs failed events.

🧾 2. Avro Schema

Path: avro/order.avsc

{
"type": "record",
"name": "Order",
"namespace": "com.student.assignment",
"fields": [
{ "name": "orderId", "type": "string" },
{ "name": "product", "type": "string" },
{ "name": "price", "type": "float" }
]
}

3.  Kafka Setup Using Docker
    Start Zookeeper
    docker run -d --name zookeeper -p 2181:2181 zookeeper

Start Kafka
docker run -d --name kafka \
 --link zookeeper:zookeeper \
 -p 9092:9092 \
 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
 wurstmeister/kafka

🎯 4. Create Kafka Topics
docker exec -it kafka kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092
docker exec -it kafka kafka-topics.sh --create --topic orders-dlq --bootstrap-server localhost:9092

Check topics:

docker exec -it kafka kafka-topics.sh --list --bootstrap-server localhost:9092

🧪 5. Install Python Dependencies
pip install -r requirements.txt

requirements.txt contains:

kafka-python
fastavro

📤 6. Run the Producer
cd producer
python producer.py

It prints:

Sent: {'orderId': '...', 'product': 'Keyboard', 'price': 123.45}

📥 7. Run the Consumer
cd consumer
python consumer.py

It prints:

Received: {...}
Running Average Price: 145.23
Retry 1/3...
Sending to DLQ...

🗑 8. Run DLQ Consumer
cd dlq
python dlq_consumer.py

It prints failed messages:

DLQ Message: { ... }
