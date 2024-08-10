import time

from confluent_kafka import Producer
from faker import Faker
import random
import json
from datetime import datetime

# Initialize Faker to generate fake data
fake = Faker()

# Kafka producer configuration
conf = {
    'bootstrap.servers': '192.168.0.102:9092',  # Replace with your Kafka broker(s)
    'client.id': 'finaldata_producer'
}

# Create a Kafka producer instance
producer = Producer(conf)

# Function to generate a single data record
def generate_data():
    mention_id = fake.uuid4()
    sentiment = random.choice([1, 2, 3, 4])
    channel = random.choice(['facebook', 'twitter', 'instagram', 'linkedin'])
    mention_time = datetime.utcnow().isoformat()

    data = {
        'mention_id': mention_id,
        'sentiment': sentiment,
        'channel': channel,
        'mention_time': mention_time
    }

    return data

# Function to deliver messages to Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce data to the Kafka topic
def produce_data():
    topic = 'finaldata'  # Replace with your topic name

    for _ in range(100):  # Generate and send 100 messages
        data = generate_data()
        producer.produce(topic, key=data['mention_id'], value=json.dumps(data), callback=delivery_report)
        producer.poll(1)
        time.sleep(1)

    # Wait for any outstanding messages to be delivered
    producer.flush()

if __name__ == "__main__":
    produce_data()
