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
    "BrandID": 905,
    "CategoryID": 47,
    "SocialID": "28d40f80-a0ad-4971-9323-cb1926e6effc",
    "NumComments": 60,
    "NumLikesCount": 10051,
    "NumShareCount": 8009,
    "NumVideoViews": 9611,
    "CreatedDate": "2024-08-09T18:17:22",
}


# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    key_data = json.dumps({
        "BrandID": data["BrandID"],
        "CategoryID": data["CategoryID"],
        "SocialID": data["SocialID"]
    })
    producer.produce(topic, key=key_data, value=json.dumps(data))
    producer.poll(1)
    print("Message Produced")
    # Ensure all messages are sent
    producer.flush()


# Usage
if __name__ == "__main__":
    topic = 'updateddata'  # Replace with your Kafka topic
    send_data_to_kafka(topic, data)
    print("Data sent to Kafka topic.")
