import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://localhost:8088"

# ksqlDB query to create the final_data_stream stream with composite keys
create_final_data_stream_query = """
CREATE STREAM ALERT_FINAL_DATA_STREAM (
    BrandID INT,
    BrandName STRING,
    CategoryGroupID INT,
    CategoryID INT,
    CategoryName STRING,
    ChannelType INT,
    ChannelGroupID INT,
    Description STRING,
    SocialID STRING,
    NumLikesORFollowers STRING,
    NumLikesCount INT,
    NumComments INT,
    NumCommentsCount INT,
    NumShareCount INT,
    NumVideoViews INT,
    ShareCount INT,
    CreatedDate STRING,
    SentimentType INT,
    PassivePositiveSentimentCount INT,
    NegativeSentimentCount INT,
    NeutralSentimentCount INT,
    Tagid STRING,
    UpperCategoryID INT,
    IsDeleted BOOLEAN,
    SimplifiedText STRING,
    Rating DOUBLE,
    IsVerified BOOLEAN,
    RetweetedStatusID BIGINT,
    InReplyToStatusId BIGINT,
    MediaType STRING,
    Reach INT,
    Impression INT,
    Engagement INT,
    CategoryXML STRING,
    MediaEnum INT,
    Lang STRING,
    LanguageName STRING,
    PostType INT,
    IsBrandPost BOOLEAN,
    InstagramPostType INT,
    SettingID INT,
    quotedTweetCounts INT,
    InfluencerCategory ARRAY<STRING>,
    TypeofComment INT,
    OrderID INT,
    IsHistoric BOOLEAN,
    MentionMD5 STRING,
    Content STRING,
    NRESentimentScore DOUBLE,
    InsertedDate STRING,
    AuthorSocialID STRING,
    AuthorName STRING,
    UserInfoScreenName STRING,
    Bio STRING,
    FollowersCount INT,
    FollowingCount INT,
    TweetCount INT,
    UserInfoIsVerified BOOLEAN,
    PicUrl STRING
) WITH (
    KAFKA_TOPIC='updatedata',
  VALUE_FORMAT='JSON',
  TIMESTAMP='CreatedDate',
  TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss'
);
"""


# Function to execute ksqlDB query
def execute_ksqldb_query(query):
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": query}
    )
    if response.status_code == 200:
        print(f"Statement executed successfully: {query}")
        print(response.json())
    else:
        print(f"Failed to execute query: {response.status_code}, {response.text}")


if __name__ == "__main__":
    # Create the final_data_stream stream
    execute_ksqldb_query(create_final_data_stream_query)
