import json
import random
import time

from confluent_kafka import Producer
from faker import Faker

# Initialize Faker
fake = Faker()

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}


ksql_query = {
    "ksql": """SELECT *, TIMESTAMPTOSTRING(CREATEDDATE, 'yyyy-MM-dd''T''HH:mm:ssXXX', 'Asia/Kolkata') AS readable_timestamp                         FROM FINAL_AGGREGATED_TABLE                          WHERE BRANDID = 12178 AND CATEGORYID = 1808                 AND CREATEDDATE >= '2024-08-20T10:42:29'                 AND (ChannelType IN (78,81));""",
    "streamsProperties": {}
}

# Create Kafka Producer
producer = Producer(producer_conf)

# Store generated parent posts to create child comments
parent_posts = []


# Function to generate fake data
def generate_fake_data(is_parent=True):
    if is_parent:
        socialid = fake.uuid4()
        parent_posts.append(socialid)
        return {
            "mentionid": fake.uuid4(),
            "uniqueid": fake.uuid4(),
            "createdate": fake.date_time_this_year().isoformat(),
            "numlikes": random.randint(0, 500),
            "numcomments": random.randint(0, 500),
            "numshare": random.randint(0, 100),
            "socialid": socialid,
            "postsocialid": "",
            "description": fake.text(max_nb_chars=200)
        }
    else:
        if not parent_posts:
            return generate_fake_data(is_parent=True)
        parent_socialid = random.choice(parent_posts)
        return {
            "mentionid": fake.uuid4(),
            "uniqueid": fake.uuid4(),
            "createdate": fake.date_time_this_year().isoformat(),
            "numlikes": random.randint(0, 500),
            "numcomments": random.randint(0, 500),
            "numshare": random.randint(0, 100),
            "socialid": fake.uuid4(),
            "postsocialid": parent_socialid,
            "description": fake.text(max_nb_chars=200)
        }


# Function to deliver report (optional)
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# Generate and send data to Kafka
def produce_data():
    try:
        while True:
            # Decide whether to generate a parent post or a child comment
            is_parent = random.choice([True, False])
            data = generate_fake_data(is_parent=is_parent)
            producer.produce('finaldata', key=data['mentionid'], value=json.dumps(data), callback=delivery_report)
            producer.poll(0)
            time.sleep(1)  # Add delay to simulate real-time data generation
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    produce_data()
