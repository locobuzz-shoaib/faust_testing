import json

from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': '172.18.244.10:9092',  # Replace with your Kafka broker
    'client.id': 'test_data_producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# JSON data to be sent
comp_key = "17612_1808_29803817-b5f0-4654-a56c-4405722e632b_98fb099c-c165-4479-a69e-0a33120d9e2e_5259223866ea0bd54ef660d2e245dfef_2024-08-20T21:10:18"
cat_id, b_id, s_id, t_id, m_md5, c_date = comp_key.split("_")
data = {
    "CompositeKey": comp_key,
    "BrandID": int(cat_id),
    "CategoryID": int(b_id),
    "SocialID": s_id,
    "Tagid": t_id,
    "MentionMD5": m_md5,
    "NUMLIKESCOUNT": 20012,
    "NUMVIDEOVIEWS": 20000,
    "NumCommentsCount": 10021,
    "Reach": 20021,
    "Impression": 20002,
    "Engagement": 20009,
    "CreatedDate": c_date,
}


# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    # key_data = json.dumps({
    #     "BrandID": data["BrandID"],
    #     "CategoryID": data["CategoryID"],
    #     "SocialID": data["SocialID"],
    #     "Tagid": data["Tagid"],
    #     "MentionMD5": data["MentionMD5"],
    #     "CreatedDate": data["CreatedDate"],
    #     "CompositeKey": data["CompositeKey"]
    # })
    producer.produce(topic, key=comp_key, value=json.dumps(data))
    producer.poll(1)
    print("Message Produced")
    # Ensure all messages are sent
    producer.flush()


# Usage
if __name__ == "__main__":
    topic = 'AlertFinalData'  # Replace with your Kafka topic
    for i in range(0, 10):
        send_data_to_kafka(topic, data)
        print("Data sent to Kafka topic.", data)
