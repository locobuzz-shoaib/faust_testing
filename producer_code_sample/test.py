import json

from confluent_kafka import Consumer, KafkaError

# Kafka Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'JOINED_STREAM',
    'auto.offset.reset': 'earliest'
}

# Create Kafka Consumer
consumer = Consumer(consumer_conf)

# Subscribe to the joined_stream topic
consumer.subscribe(['JOINED_STREAM'])


# Function to handle messages
def handle_message(msg):
    try:
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {json.dumps(data, indent=2)}")
        # Add your logic to process the data here
    except Exception as e:
        print(f"Failed to process message: {e}")


if __name__ == "__main__":
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break

            handle_message(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
