import json

from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': '192.168.0.107:9092',  # Replace with your Kafka broker
    'client.id': 'test_data_producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# JSON data to be sent
comp_key = "17612_1808_f434f341-34ce-44d9-b3c6-4a0311ccea02_7af17e95-37db-4a32-9bc9-f7756b0fb0e4_e587408ff3eaa704fd9f3bc7f7c87e17_2024-08-18T12:37:49"
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
    send_data_to_kafka(topic, data)
    print("Data sent to Kafka topic.", data)
