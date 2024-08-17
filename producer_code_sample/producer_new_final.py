import json
import random
import time
from datetime import datetime,  time as dt_time
from confluent_kafka import Producer
from faker import Faker

# Kafka configuration
conf = {
    'bootstrap.servers': '192.168.0.107:9092',  # Update with your Kafka broker(s)
    'client.id': 'alert_data_producer'
}

# Initialize Kafka producer
producer = Producer(conf)

# Initialize Faker instance
faker = Faker()


# Function to generate random data
def generate_alert_data(start_date):
    data = {
        "CompositeKey": "",
        "BrandID": 17612,
        "BrandName": faker.company(),
        "CategoryGroupID": random.randint(1, 100),
        "CategoryID": 1808,
        "CategoryName": faker.word(),
        "ChannelType": random.randint(1, 10),
        "ChannelGroupID": random.randint(1, 10),
        "Description": faker.sentence(),
        "Caption": faker.sentence(),
        "ScreenName": faker.user_name(),
        "SocialID": faker.uuid4(),
        "ObjectID": faker.uuid4(),
        "NumLikesORFollowers": str(random.randint(0, 10000)),
        "NumLikesCount": random.randint(0, 10000),
        "NumComments": random.randint(0, 10000),
        "NumCommentsCount": random.randint(0, 10000),
        "NumShareCount": random.randint(0, 10000),
        "NumVideoViews": random.randint(0, 10000),
        "ShareCount": random.randint(0, 10000),
        "CreatedDate": start_date,
        "SentimentType": random.randint(0, 2),
        "PassivePositiveSentimentCount": random.randint(0, 100),
        "NegativeSentimentCount": random.randint(0, 100),
        "NeutralSentimentCount": random.randint(0, 100),
        "Tagid": faker.uuid4(),
        "UpperCategoryID": random.randint(1, 100),
        "IsDeleted": random.choice([True, False]),
        "SimplifiedText": faker.text(),
        "Rating": round(random.uniform(1, 5), 2),
        "IsVerified": random.choice([True, False]),
        "RetweetedStatusID": random.randint(0, 1000000),
        "InReplyToStatusId": random.randint(0, 1000000),
        "MediaType": faker.word(),
        "Reach": random.randint(0, 1000000),
        "Impression": random.randint(0, 1000000),
        "Engagement": random.randint(0, 1000000),
        "CategoryXML": "<category>{}</category>".format(faker.word()),
        "PostSocialID": faker.uuid4(),
        "ParentSocialID": faker.uuid4(),
        "MediaEnum": random.randint(1, 10),
        "Lang": faker.language_code(),
        "LanguageName": faker.language_name(),
        "PostType": random.randint(1, 10),
        "IsBrandPost": random.choice([True, False]),
        "InstagramPostType": random.randint(1, 10),
        "SettingID": random.randint(1, 1000),
        "FilterKeywords": faker.word(),
        "quotedTweetCounts": random.randint(0, 100),
        "InfluencerCategory": [faker.word() for _ in range(random.randint(1, 5))],
        "InfluencerCategoryID": [faker.uuid4() for _ in range(random.randint(1, 5))],
        "InfluencerCategoryName": [faker.word() for _ in range(random.randint(1, 5))],
        "EntitySentimentJson": json.dumps({"sentiment": random.choice(["positive", "neutral", "negative"])}),
        "TypeofComment": random.randint(1, 10),
        "EmotionScoreJson": json.dumps({"emotion": random.choice(["joy", "anger", "sadness"])}),
        "ToxicityScoreJson": json.dumps({"toxicity": round(random.uniform(0, 1), 2)}),
        "Categoryjson": json.dumps({"category": faker.word()}),
        "NLPSentiContent": faker.sentence(),
        "OrderID": random.randint(1, 1000),
        "IsHistoric": random.choice([True, False]),
        "Hastagcloud": [faker.word() for _ in range(random.randint(1, 5))],
        "MentionMD5": faker.md5(),
        "Content": faker.text(),
        "NRESentimentScore": round(random.uniform(0, 1), 2),
        "Contextualtagcloud": [faker.word() for _ in range(random.randint(1, 5))],
        "Keywordtagcloud": [faker.word() for _ in range(random.randint(1, 5))],
        "Emojitagcloud": [faker.word() for _ in range(random.randint(1, 5))],
        "InsertedDate": start_date,
        "AuthorSocialID": faker.uuid4(),
        "AuthorName": faker.name(),
        "UserInfoScreenName": faker.user_name(),
        "UserSentiment": random.randint(1, 10),
        "Bio": faker.text(),
        "FollowersCount": random.randint(0, 10000),
        "FollowingCount": random.randint(0, 10000),
        "TweetCount": random.randint(0, 10000),
        "UserInfoIsVerified": random.choice([True, False]),
        "PicUrl": faker.image_url(),
        "URL": faker.url(),
        "AttachmentXML": (
            "<Attachments><Item><Name>ThumbnailURL</Name><MediaType>3</MediaType>"
            "<Url>http://www.youtube.com/watch?v=nb5s-4mEUto</Url>"
            "<ThumbUrl>https://s3.amazonaws.com/locobuzz.socialimages/348d9162-abfd-469e-a165-847b9520a029_1.jpg</ThumbUrl>"
            "<json/></Item></Attachments>"
        ),
        "ConversationId": faker.uuid4(),
        "Title": faker.sentence()
    }

    return data


# Function to send data to Kafka topic
def send_alert_data_to_kafka(topic, start_date, num_messages=10):
    for _ in range(num_messages):
        current_time = generate_random_datetime_on_same_day2(start_date, start_time=dt_time(9, 0), end_time=dt_time(17, 0))
        data = generate_alert_data(current_time)
        composite_key = f"{data['BrandID']}_{data['CategoryID']}_{data['SocialID']}_{data['Tagid']}_{data['MentionMD5']}_{current_time}"
        data["CompositeKey"] = composite_key

        producer.produce(topic, key=composite_key, value=json.dumps(data))
        producer.poll(1)
        time.sleep(1)
        print(f"Message Produced: {data['CompositeKey']}")

    # Ensure all messages are sent
    producer.flush()


# Function to generate a random datetime within the same day
def generate_random_datetime_on_same_day(date):
    start_datetime = datetime.combine(date, datetime.min.time())  # Start of the day
    end_datetime = datetime.combine(date, datetime.max.time())  # End of the day
    random_datetime = faker.date_time_between(start_date=start_datetime, end_date=end_datetime)
    formatted_datetime = random_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    return formatted_datetime

def generate_random_datetime_on_same_day2(date, start_time, end_time):
    start_datetime = datetime.combine(date, start_time)  # Start time of the range
    end_datetime = datetime.combine(date, end_time)  # End time of the range
    random_datetime = faker.date_time_between(start_date=start_datetime, end_date=end_datetime)
    formatted_datetime = random_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    return formatted_datetime


# Usage
if __name__ == "__main__":
    start_date = datetime(2024, 8, 17)
    send_alert_data_to_kafka('AlertFinalData', start_date, num_messages=10)
    print("Data sent to Kafka topic.")
