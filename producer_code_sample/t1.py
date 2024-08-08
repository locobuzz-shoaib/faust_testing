    import json
from datetime import datetime

from confluent_kafka import Producer

# Kafka Producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(**conf)


# Delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


# Function to send data to Kafka topic
def send_data_to_kafka(topic, data):
    producer.produce(topic, value=json.dumps(data), callback=delivery_report)
    producer.flush()


# Example data to push to Kafka topic
data = {
    "BRANDID": 908089,
    "BRANDNAME": "Example Brand",
    "CATEGORYGROUPID": 1,
    "CATEGORYID": 2,
    "CATEGORYNAME": "Example Category",
    "CHANNELTYPE": 3,
    "CHANNELGROUPID": 4,
    "DESCRIPTION": "Example description",
    "SOCIALID": "social123",
    "OBJECTID": "object123",
    "NUMLIKESCOUNT": 100,
    "NUMCOMMENTS": 20,
    "NUMCOMMENTSCOUNT": 20,
    "NUMSHARECOUNT": 5,
    "NUMVIDEOVIEWS": 1000,
    "SHARECOUNT": 5,
    "CREATEDDATE": datetime.now().isoformat(),
    "SENTIMENTTYPE": 1,
    "PASSIVEPOSITIVESENTIMENTCOUNT": 50,
    "NEGATIVESENTIMENTCOUNT": 10,
    "NEUTRALSENTIMENTCOUNT": 40,
    "TAGID": "tag123",
    "UPPERCATEGORYID": 5,
    "ISDELETED": False,
    "SIMPLIFIEDTEXT": "Simplified text example",
    "RATING": 4,
    "ISVERIFIED": True,
    "RETWEETEDSTATUSID": 12345,
    "INREPLYTOSTATUSID": 54321,
    "MEDIATYPE": "image",
    "REACH": 5000,
    "IMPRESSION": 3000,
    "ENGAGEMENT": 200,
    "CATEGORYXML": "<category>example</category>",
    "MEDIAENUM": 1,
    "LANG": "en",
    "LANGUAGENAME": "English",
    "POSTTYPE": 1,
    "ISBRANDPOST": True,
    "INSTAGRAMPOSTTYPE": 2,
    "SETTINGID": 6,
    "QUOTEDTWEETCOUNTS": 3,
    "INFLUENCERCATEGORY": ["category1", "category2"],
    "TYPEOFCOMMENT": 1,
    "CATEGORYJSON": '{"category": "example"}',
    "NLPSENTICONTENT": "example content",
    "ORDERID": 789,
    "ISHISTORIC": False,
    "HASTAGCLOUD": ["tag1", "tag2"],
    "MENTIONMD5": "md5123",
    "CONTENT": "Example content",
    "NRESENTIMENTSCORE": 0.75,
    "CONTEXTUALTAGCLOUD": ["context1", "context2"],
    "KEYWORDTAGCLOUD": ["keyword1", "keyword2"],
    "EMOJITAGCLOUD": ["emoji1", "emoji2"],
    "AUTHORSOCIALID": "author123",
    "AUTHORNAME": "Author Name",
    "USERSENTIMENT": 1,
    "FOLLOWERSCOUNT": 1000,
    "FOLLOWINGCOUNT": 200,
    "TWEETCOUNT": 300,
    "USERINFOISVERIFIED": True
}

# Sending the data
send_data_to_kafka('AlertFinalData', data=data)

print("Data pushed to Kafka topic successfully.")
