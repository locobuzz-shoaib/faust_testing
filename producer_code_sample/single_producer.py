import json
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': '192.168.0.102:9092',  # Replace with your Kafka broker
    'client.id': 'test_data_producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# JSON data to be sent
data = {
    "SOCIALID": "161b4d21-3162-443b-9a1f-6634acce4adb",
    "NUMLIKESCOUNT": 19998,
    "NUMCOMMENTS": 5428,
    "NUMCOMMENTSCOUNT": 5001,
    "NUMSHARECOUNT": 8009,
    "NUMVIDEOVIEWS": 9611,
    "CREATEDDATE": "2024-08-09T13:44:58"
}

# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    producer.produce(topic, value=json.dumps(data))
    producer.poll(1)
    print("Message Produced")
    # Ensure all messages are sent
    producer.flush()

# Usage
if __name__ == "__main__":
    topic = 'updateddata'  # Replace with your Kafka topic
    send_data_to_kafka(topic, data)
    print("Data sent to Kafka topic.")
