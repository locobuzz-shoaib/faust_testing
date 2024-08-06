from confluent_kafka import Producer
from faker import Faker
import json
import random
import time
from datetime import datetime

# Initialize Faker
fake = Faker()

# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Create Kafka Producer
producer = Producer(producer_conf)

# Specific date range
start_date = datetime(2024, 9, 4)
end_date = datetime(2024, 9, 6)


# Function to generate fake finaldata
def generate_fake_finaldata(socialid=None):
    return {
        "mentionid": fake.uuid4(),
        "uniqueid": fake.uuid4(),
        "createdate": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
        "numlikes": random.randint(0, 500),
        "numcomments": random.randint(0, 100),
        "numshare": random.randint(0, 50),
        "socialid": socialid,
        "postsocialid": fake.uuid4(),
        "brandid": fake.uuid4(),
        "categoryid": fake.uuid4(),
        "description": fake.text(max_nb_chars=200)
    }


# Function to generate fake updateddata
def generate_fake_updateddata(postsocialid=None):
    return {
        "mentionid": fake.uuid4(),
        "uniqueid": fake.uuid4(),
        "brandid": fake.uuid4(),
        "socialid": None if random.random() > 0.5 else fake.uuid4(),
        "postsocialid": postsocialid,
        "updatedate": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
        "numlikes": random.randint(0, 500),
        "numcomments": random.randint(0, 100),
        "numshare": random.randint(0, 50),
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
        for _ in range(100):
            # Generate some matching socialid and postsocialid
            matching_socialid = fake.uuid4()

            # Generate finaldata with matching socialid
            finaldata_matching = generate_fake_finaldata(socialid=matching_socialid)
            updateddata_matching = generate_fake_updateddata(postsocialid=matching_socialid)

            # Produce matching records
            producer.produce('finaldata', key=finaldata_matching['mentionid'], value=json.dumps(finaldata_matching),
                             callback=delivery_report)
            producer.produce('updateddata', key=updateddata_matching['mentionid'],
                             value=json.dumps(updateddata_matching), callback=delivery_report)

            # Generate finaldata without matching socialid
            finaldata_non_matching = generate_fake_finaldata()
            updateddata_non_matching = generate_fake_updateddata(fake.uuid4())

            # Produce non-matching records
            producer.produce('finaldata', key=finaldata_non_matching['mentionid'],
                             value=json.dumps(finaldata_non_matching), callback=delivery_report)
            producer.produce('updateddata', key=updateddata_non_matching['mentionid'],
                             value=json.dumps(updateddata_non_matching), callback=delivery_report)

            producer.poll(0)
            time.sleep(1)  # Add delay to simulate real-time data generation
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    produce_data()
