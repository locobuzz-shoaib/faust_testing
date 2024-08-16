import json

from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': '192.168.0.178:9092',  # Replace with your Kafka broker
    'client.id': 'test_data_producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# JSON data to be sent
comp_key = "565_55_04628ed1-b39f-4958-a3eb-a63dcb8af3be_e4c0c39c-0fee-4286-b494-4014a6cee2d7_168346504f751a24814dd67a135efff5_2024-08-09T15:31:21"
cat_id, b_id, s_id, t_id, m_md5, c_date = comp_key.split("_")
data = {
    "COMPOSITE_KEY": comp_key,
    "BrandID": int(cat_id),
    "CategoryID": int(b_id),
    "SocialID": s_id,
    "Tagid": t_id,
    "MentionMD5": m_md5,
    "NUMLIKESCOUNT": 60000,
    "NUMVIDEOVIEWS": 20000,
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
    #     "COMPOSITE_KEY": data["COMPOSITE_KEY"]
    # })
    producer.produce(topic, key=comp_key, value=json.dumps(data))
    producer.poll(1)
    print("Message Produced")
    # Ensure all messages are sent
    producer.flush()


# Usage
if __name__ == "__main__":
    topic = 'aggregated_topic'  # Replace with your Kafka topic
    send_data_to_kafka(topic, data)
    print("Data sent to Kafka topic.", data)
