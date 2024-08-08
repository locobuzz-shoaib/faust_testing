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
counter = 0
counter2= 0
# Create Kafka Producer
producer = Producer(producer_conf)

# Specific date range
start_date = datetime(2024, 9, 6)
end_date = datetime(2024, 9, 7)

# Function to generate fake final data for ALERT_FINAL_DATA_STREAM
def generate_fake_finaldata(socialid=None):
    global counter
    counter = counter+1
    return {
        "BRANDID": random.randint(1, 1000),
        "BRANDNAME": fake.company(),
        "CATEGORYGROUPID": random.randint(1, 100),
        "CATEGORYID": random.randint(1, 100),
        "CATEGORYNAME": fake.word(),
        "CHANNELTYPE": random.randint(1, 10),
        "CHANNELGROUPID": random.randint(1, 10),
        "DESCRIPTION": fake.text(max_nb_chars=200),
        "SOCIALID": socialid or fake.uuid4(),
        "OBJECTID": fake.uuid4(),
        "NUMLIKESCOUNT": counter,
        "NUMCOMMENTS": random.randint(0, 100),
        "NUMCOMMENTSCOUNT": random.randint(0, 100),
        "NUMSHARECOUNT": random.randint(0, 50),
        "NUMVIDEOVIEWS": random.randint(0, 1000),
        "SHARECOUNT": random.randint(0, 50),
        "CREATEDDATE": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
        "SENTIMENTTYPE": random.randint(1, 3),
        "PASSIVEPOSITIVESENTIMENTCOUNT": random.randint(0, 100),
        "NEGATIVESENTIMENTCOUNT": random.randint(0, 100),
        "NEUTRALSENTIMENTCOUNT": random.randint(0, 100),
        "TAGID": fake.uuid4(),
        "UPPERCATEGORYID": random.randint(1, 100),
        "ISDELETED": fake.boolean(),
        "SIMPLIFIEDTEXT": fake.text(max_nb_chars=200),
        "RATING": random.randint(1, 5),
        "ISVERIFIED": fake.boolean(),
        "RETWEETEDSTATUSID": random.randint(1, 1000),
        "INREPLYTOSTATUSID": random.randint(1, 1000),
        "MEDIATYPE": fake.word(),
        "REACH": random.randint(1, 10000),
        "IMPRESSION": random.randint(1, 10000),
        "ENGAGEMENT": random.randint(1, 1000),
        "CATEGORYXML": "<category>" + fake.word() + "</category>",
        "MEDIAENUM": random.randint(1, 5),
        "LANG": fake.language_code(),
        "LANGUAGENAME": fake.language_name(),
        "POSTTYPE": random.randint(1, 5),
        "ISBRANDPOST": fake.boolean(),
        "INSTAGRAMPOSTTYPE": random.randint(1, 5),
        "SETTINGID": random.randint(1, 1000),
        "QUOTEDTWEETCOUNTS": random.randint(0, 100),
        "INFLUENCERCATEGORY": [fake.word() for _ in range(5)],
        "TYPEOFCOMMENT": random.randint(1, 5),
        "CATEGORYJSON": json.dumps({fake.word(): fake.word()}),
        "NLPSENTICONTENT": fake.text(max_nb_chars=200),
        "ORDERID": random.randint(1, 1000),
        "ISHISTORIC": fake.boolean(),
        "HASTAGCLOUD": [fake.word() for _ in range(5)],
        "MENTIONMD5": fake.md5(),
        "CONTENT": fake.text(max_nb_chars=200),
        "NRESENTIMENTSCORE": random.uniform(0, 1),
        "CONTEXTUALTAGCLOUD": [fake.word() for _ in range(5)],
        "KEYWORDTAGCLOUD": [fake.word() for _ in range(5)],
        "EMOJITAGCLOUD": [fake.emoji() for _ in range(5)],
        "AUTHORSOCIALID": fake.uuid4(),
        "AUTHORNAME": fake.name(),
        "USERSENTIMENT": random.randint(1, 5),
        "FOLLOWERSCOUNT": random.randint(0, 10000),
        "FOLLOWINGCOUNT": random.randint(0, 1000),
        "TWEETCOUNT": random.randint(0, 10000),
        "USERINFOISVERIFIED": fake.boolean()
    }

# Function to generate fake updated data for ALERT_UPDATE_DATA_STREAM
def generate_fake_updateddata(postsocialid=None):
    global counter2
    return {
        "BRANDID": random.randint(1, 1000),
        "BRANDNAME": fake.company(),
        "CATEGORYGROUPID": random.randint(1, 100),
        "CATEGORYID": random.randint(1, 100),
        "CATEGORYNAME": fake.word(),
        "CHANNELTYPE": random.randint(1, 10),
        "CHANNELGROUPID": random.randint(1, 10),
        "DESCRIPTION": fake.text(max_nb_chars=200),
        "SOCIALID": fake.uuid4(),
        "POSTSOCIALID": postsocialid,
        "OBJECTID": fake.uuid4(),
        "NUMLIKESCOUNT": random.randint(0, 100),
        "NUMCOMMENTS": counter,
        "NUMCOMMENTSCOUNT": random.randint(0, 100),
        "NUMSHARECOUNT": random.randint(0, 50),
        "NUMVIDEOVIEWS": random.randint(0, 1000),
        "SHARECOUNT": random.randint(0, 50),
        "CREATEDDATE": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
        "SENTIMENTTYPE": random.randint(1, 3),
        "PASSIVEPOSITIVESENTIMENTCOUNT": random.randint(0, 100),
        "NEGATIVESENTIMENTCOUNT": random.randint(0, 100),
        "NEUTRALSENTIMENTCOUNT": random.randint(0, 100),
        "TAGID": fake.uuid4(),
        "UPPERCATEGORYID": random.randint(1, 100),
        "ISDELETED": fake.boolean(),
        "SIMPLIFIEDTEXT": fake.text(max_nb_chars=200),
        "RATING": random.randint(1, 5),
        "ISVERIFIED": fake.boolean(),
        "RETWEETEDSTATUSID": random.randint(1, 1000),
        "INREPLYTOSTATUSID": random.randint(1, 1000),
        "MEDIATYPE": fake.word(),
        "REACH": random.randint(1, 10000),
        "IMPRESSION": random.randint(1, 10000),
        "ENGAGEMENT": random.randint(1, 1000),
        "CATEGORYXML": "<category>" + fake.word() + "</category>",
        "MEDIAENUM": random.randint(1, 5),
        "LANG": fake.language_code(),
        "LANGUAGENAME": fake.language_name(),
        "POSTTYPE": random.randint(1, 5),
        "ISBRANDPOST": fake.boolean(),
        "INSTAGRAMPOSTTYPE": random.randint(1, 5),
        "SETTINGID": random.randint(1, 1000),
        "QUOTEDTWEETCOUNTS": random.randint(0, 100),
        "INFLUENCERCATEGORY": [fake.word() for _ in range(5)],
        "TYPEOFCOMMENT": random.randint(1, 5),
        "CATEGORYJSON": json.dumps({fake.word(): fake.word()}),
        "NLPSENTICONTENT": fake.text(max_nb_chars=200),
        "ORDERID": random.randint(1, 1000),
        "ISHISTORIC": fake.boolean(),
        "HASTAGCLOUD": [fake.word() for _ in range(5)],
        "MENTIONMD5": fake.md5(),
        "CONTENT": fake.text(max_nb_chars=200),
        "NRESENTIMENTSCORE": random.uniform(0, 1),
        "CONTEXTUALTAGCLOUD": [fake.word() for _ in range(5)],
        "KEYWORDTAGCLOUD": [fake.word() for _ in range(5)],
        "EMOJITAGCLOUD": [fake.emoji() for _ in range(5)],
        "AUTHORSOCIALID": fake.uuid4(),
        "AUTHORNAME": fake.name(),
        "USERSENTIMENT": random.randint(1, 5),
        "FOLLOWERSCOUNT": random.randint(0, 10000),
        "FOLLOWINGCOUNT": random.randint(0, 1000),
        "TWEETCOUNT": random.randint(0, 10000),
        "USERINFOISVERIFIED": fake.boolean()
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
            producer.produce('AlertFinalData',  value=json.dumps(finaldata_matching),
                             callback=delivery_report)
            producer.produce('AlertUpdatedData',
                             value=json.dumps(updateddata_matching), callback=delivery_report)

            # Generate finaldata without matching socialid
            finaldata_non_matching = generate_fake_finaldata()
            updateddata_non_matching = generate_fake_updateddata(fake.uuid4())

            # Produce non-matching records
            producer.produce('AlertFinalData',
                             value=json.dumps(finaldata_non_matching), callback=delivery_report)
            producer.produce('AlertUpdatedData',
                             value=json.dumps(updateddata_non_matching), callback=delivery_report)

            producer.poll(0)
            time.sleep(1)  # Add delay to simulate real-time data generation
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    produce_data()
