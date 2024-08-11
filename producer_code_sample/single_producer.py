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
    "COMPOSITE_KEY": "587_73_0be18847-353d-4dbb-8245-9cc837eb511b_3c7de4d6-80f7-4ca7-ae3b-57e55182acc8_c6b3d4ce9d750cbb91fdd84241858cf6_2024-08-09T03:59:04",
    "BrandID": 587,
    "CategoryID": 73,
    "SocialID": "0be18847-353d-4dbb-8245-9cc837eb511b",
    "Tagid": "3c7de4d6-80f7-4ca7-ae3b-57e55182acc8",
    "MentionMD5": "c6b3d4ce9d750cbb91fdd84241858cf6",

    "NUMLIKESCOUNT": 5782,

    "NUMVIDEOVIEWS": 7150,
    "Reach": 461186,
    "Impression": 307998,
    "Engagement": 351864,
    "CreatedDate": "2024-08-09T03:59:04",
}


# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    key_data = json.dumps({
        "BrandID": data["BrandID"],
        "CategoryID": data["CategoryID"],
        "SocialID": data["SocialID"],
        "Tagid": data["Tagid"],
        "MentionMD5": data["MentionMD5"],
        "CreatedDate": data["CreatedDate"],
        "COMPOSITE_KEY": data["COMPOSITE_KEY"]
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
