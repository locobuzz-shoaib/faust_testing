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
comp_key = "945_72_256e15cc-43a7-4087-8ccb-1368e0549440_dd56a730-9b13-43ea-902d-9d0cc479f95f_391d93fa765212f05050457907ca4a7f_2024-08-09T11:23:05"
cat_id, b_id, s_id, t_id, m_md5, c_date = comp_key.split("_")
data = {
    "CompositeKey": comp_key,
    "BrandID": int(cat_id),
    "CategoryID": int(b_id),
    "SocialID": s_id,
    "Tagid": t_id,
    "MentionMD5": m_md5,
    "NUMLIKESCOUNT": 12121,
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
