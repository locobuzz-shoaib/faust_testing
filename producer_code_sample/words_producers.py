import time
import json
from confluent_kafka import Producer
from faker import Faker

# Kafka configuration
KAFKA_TOPIC = 'words_topic'
KAFKA_BOOTSTRAP_SERVERS = '172.18.244.10:9092'

# Create a Faker instance
faker = Faker()

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})


def delivery_report(err, msg):
    """ Callback called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_fake_data(num_messages):
    for _ in range(num_messages):
        # Generate a fake word
        word = faker.word()

        # Create a message
        message = {'word': word}

        # Produce the message to the Kafka topic
        producer.produce(KAFKA_TOPIC, key=None, value=json.dumps(message), callback=delivery_report)

        # Wait for any outstanding messages to be delivered
        producer.flush()

        # Sleep for a while before sending the next message
        time.sleep(1)


if __name__ == '__main__':
    print(f'Starting to produce 1000 fake data messages to Kafka topic: {KAFKA_TOPIC}')
    produce_fake_data(1000)
    print('Finished producing 1000 messages.')
