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
    "BrandID": 397,
    "CategoryID": 29,
    "SocialID": "19452808-6a99-434f-bb20-5e800e901def",
    "Tagid": "ed15a45a-e12b-4921-b926-b2a217215fd6",
    "MentionMD5": "7cd946375d1eb853c2c7766b96ec7de2",

    "NumShareCount": 1222,

    "Reach": 496506,
    "Impression": 22717,
    "Engagement": 219420,
    "CreatedDate": "2024-08-09T22:51:00",
}


# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    key_data = json.dumps({
        "BrandID": data["BrandID"],
        "CategoryID": data["CategoryID"],
        "SocialID": data["SocialID"],
        "Tagid": data["Tagid"],
        "MentionMD5": data["MentionMD5"],
        "CreatedDate": data["CreatedDate"]
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
