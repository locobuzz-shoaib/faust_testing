import requests

# KSQLDB_SERVER_URL = "http://a96f0aef8e7624f22a07fc3fc3ad88f2-1060712106.ap-south-1.elb.amazonaws.com"
# ksqlDB server URL
KSQLDB_SERVER_URL = "http://192.168.0.107:8088"
#
# ksqlDB query to create the final_data_stream stream with composite keys
create_final_data_stream_query = """
CREATE STREAM ALERT_FINAL_DATA_STREAM (
  CompositeKey STRING key,
  BrandID BIGINT,
  BrandName STRING,
  CategoryGroupID BIGINT,
  CategoryID BIGINT,
  CategoryName STRING,
  ChannelType INT, 
  ChannelGroupID INT,
  Description STRING,
  Caption STRING,
  ScreenName STRING,
  SocialID STRING,
  ObjectID STRING,
  NumLikesORFollowers STRING,
  NumLikesCount BIGINT,
  NumComments BIGINT,
  NumCommentsCount BIGINT,
  NumShareCount BIGINT,
  NumVideoViews BIGINT,
  ShareCount BIGINT,
  CreatedDate STRING,  
  SentimentType INT,  
  PassivePositiveSentimentCount INT,
  NegativeSentimentCount INT,
  NeutralSentimentCount INT,
  Tagid STRING,
  UpperCategoryID INT,
  IsDeleted BOOLEAN,
  SimplifiedText STRING,
  Rating DECIMAL(10, 2),
  IsVerified BOOLEAN,
  RetweetedStatusID BIGINT,
  InReplyToStatusId BIGINT,
  MediaType STRING,
  Reach BIGINT,
  Impression BIGINT,
  Engagement BIGINT,
  CategoryXML STRING,
  PostSocialID STRING,
  ParentSocialID STRING,
  MediaEnum INT, 
  Lang STRING,
  LanguageName STRING,
  PostType INT,  
  IsBrandPost BOOLEAN,
  InstagramPostType INT, 
  SettingID BIGINT,
  FilterKeywords STRING,
  quotedTweetCounts BIGINT,
  InfluencerCategory ARRAY<STRING>,  
  InfluencerCategoryID ARRAY<STRING>,
  InfluencerCategoryName ARRAY<STRING>,
  EntitySentimentJson STRING,
  TypeofComment INT,  
  EmotionScoreJson STRING,
  ToxicityScoreJson STRING,
  Categoryjson STRING,
  NLPSentiContent STRING,
  OrderID BIGINT,
  IsHistoric BOOLEAN,
  Hastagcloud ARRAY<STRING>,  
  MentionMD5 STRING,
  Content STRING,
  NRESentimentScore DOUBLE,
  Contextualtagcloud ARRAY<STRING>,  
  Keywordtagcloud ARRAY<STRING>,  
  Emojitagcloud ARRAY<STRING>, 
  InsertedDate STRING, 
  AuthorSocialID STRING,
  AuthorName STRING,
  UserInfoScreenName STRING,
  UserSentiment INT,
  Bio STRING,
  FollowersCount BIGINT,
  FollowingCount BIGINT,
  TweetCount BIGINT,
  UserInfoIsVerified BOOLEAN,
  PicUrl STRING,
  URL STRING,
  AttachmentXML STRING,
  ConversationId STRING,
  Title STRING
)
WITH ( 
  KAFKA_TOPIC='AlertFinalData',
  VALUE_FORMAT='JSON',
  KEY_FORMAT='KAFKA',
  TIMESTAMP='CreatedDate', 
  TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss'
);
"""

create_update_data_stream_query = """
CREATE OR REPLACE STREAM ALERT_UPDATED_DATA_STREAM (
    Composite_Key STRING key,
    BrandID INT key,
    BrandName STRING,
    CategoryGroupID INT,
    CategoryID INT key,
    CategoryName STRING,
    ChannelType INT,
    ChannelGroupID INT,
    SocialID STRING key,
    NumLikesORFollowers STRING,
    NumLikesCount INT,
    NumComments INT,
    NumCommentsCount INT,
    NumShareCount INT,
    NumVideoViews INT,
    ShareCount INT,
    CreatedDate STRING key,
    SentimentType INT,
    PassivePositiveSentimentCount INT,
    NegativeSentimentCount INT,
    NeutralSentimentCount INT,
    Tagid STRING key,
    UpperCategoryID INT,
    IsDeleted BOOLEAN,
    SimplifiedText STRING,
    Rating DOUBLE,
    IsVerified BOOLEAN,
    RetweetedStatusID BIGINT,
    MediaType STRING,
    Reach INT,
    Impression INT,
    Engagement INT,
    CategoryXML STRING,
    MediaEnum INT,
    PostType INT,
    IsBrandPost BOOLEAN,
    InstagramPostType INT,
    SettingID INT,
    quotedTweetCounts INT,
    InfluencerCategory ARRAY<STRING>,
    TypeofComment INT,
    OrderID INT,
    IsHistoric BOOLEAN,
    MentionMD5 STRING key,
    Content STRING,
    NRESentimentScore DOUBLE,
    FollowersCount INT,
    FollowingCount INT,
    TweetCount INT
) WITH (
  KAFKA_TOPIC='updateddata',
  VALUE_FORMAT='JSON',
  TIMESTAMP='CreatedDate',
  KEY_FORMAT='JSON',
  TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss'    
);
"""
create_joined_stream = """
CREATE OR REPLACE STREAM LATEST_AGGREGATED_ALERT_STREAM AS
SELECT 
    a.BrandID AS BrandID,
    a.BrandName AS BrandName,
    a.CategoryGroupID AS CategoryGroupID,
    a.CategoryID AS CategoryID,
    a.CategoryName AS CategoryName,
    a.ChannelType AS ChannelType,
    a.ChannelGroupID AS ChannelGroupID,
    a.Description AS Description,
    a.SocialID AS SocialID,
    a.NumLikesORFollowers AS NumLikesORFollowers,
    LATEST_BY_OFFSET(COALESCE(u.NumLikesCount, a.NumLikesCount)) AS NumLikesCount, 
    LATEST_BY_OFFSET(COALESCE(u.NumComments, a.NumComments)) AS NumComments,
    LATEST_BY_OFFSET(COALESCE(u.NumCommentsCount, a.NumCommentsCount)) AS NumCommentsCount,
    LATEST_BY_OFFSET(COALESCE(u.NumShareCount, a.NumShareCount)) AS NumShareCount,
    LATEST_BY_OFFSET(COALESCE(u.NumVideoViews, a.NumVideoViews)) AS NumVideoViews,
    a.ShareCount AS ShareCount,
    LATEST_BY_OFFSET(a.CreatedDate) AS CreatedDate,
    a.SentimentType AS SentimentType,
    a.PassivePositiveSentimentCount AS PassivePositiveSentimentCount,
    a.NegativeSentimentCount AS NegativeSentimentCount,
    a.NeutralSentimentCount AS NeutralSentimentCount,
    a.Tagid AS Tagid,
    a.UpperCategoryID AS UpperCategoryID,
    a.IsDeleted AS IsDeleted,
    a.SimplifiedText AS SimplifiedText,
    a.Rating AS Rating,
    a.IsVerified AS IsVerified,
    a.RetweetedStatusID AS RetweetedStatusID,
    a.InReplyToStatusId AS InReplyToStatusId,
    a.MediaType AS MediaType,
    a.Reach AS Reach,
    a.Impression AS Impression,
    a.Engagement AS Engagement,
    a.CategoryXML AS CategoryXML,
    a.MediaEnum AS MediaEnum,
    a.Lang AS Lang,
    a.LanguageName AS LanguageName,
    a.PostType AS PostType,
    a.IsBrandPost AS IsBrandPost,
    a.InstagramPostType AS InstagramPostType,
    a.SettingID AS SettingID,
    a.quotedTweetCounts AS quotedTweetCounts,
    a.InfluencerCategory AS InfluencerCategory,
    a.TypeofComment AS TypeofComment,
    a.OrderID AS OrderID,
    a.IsHistoric AS IsHistoric,
    a.MentionMD5 AS MentionMD5,
    a.Content AS Content,
    a.NRESentimentScore AS NRESentimentScore,
    a.InsertedDate AS InsertedDate,
    a.AuthorSocialID AS AuthorSocialID,
    a.AuthorName AS AuthorName,
    a.UserInfoScreenName AS UserInfoScreenName,
    a.Bio AS Bio,
    a.FollowersCount AS FollowersCount,
    a.FollowingCount AS FollowingCount,
    a.TweetCount AS TweetCount,
    a.UserInfoIsVerified AS UserInfoIsVerified,
    a.PicUrl AS PicUrl,
    a.AttachmentXML AS AttachmentXML,
    u.BrandID
FROM ALERT_FINAL_DATA_STREAM a
LEFT JOIN ALERT_UPDATED_DATA_STREAM u
WITHIN 72 HOURS
  ON a.SocialID = u.SocialID
WHERE a.CategoryID = u.CategoryID AND a.BrandID = u.BrandID
GROUP BY a.BrandID, a.CategoryID
EMIT CHANGES;
"""

filterd_stream = """
CREATE STREAM FILTERED_ALERT_STREAM AS
SELECT 
    BrandID,
    BrandName,
    CategoryGroupID,
    CategoryID,
    CategoryName,
    ChannelType,
    ChannelGroupID,
    Description,
    SocialID,
    NumLikesORFollowers,
    NumLikesCount, 
    NumComments,
    NumCommentsCount,
    NumShareCount,
    NumVideoViews,
    ShareCount,
    CreatedDate,
    SentimentType,
    PassivePositiveSentimentCount,
    NegativeSentimentCount,
    NeutralSentimentCount,
    Tagid,
    UpperCategoryID,
    IsDeleted,
    SimplifiedText,
    Rating,
    IsVerified,
    RetweetedStatusID,
    InReplyToStatusId,
    MediaType,
    Reach,
    Impression,
    Engagement,
    CategoryXML,
    MediaEnum,
    Lang,
    LanguageName,
    PostType,
    IsBrandPost,
    InstagramPostType,
    SettingID,
    quotedTweetCounts,
    InfluencerCategory,
    TypeofComment,
    OrderID,
    IsHistoric,
    MentionMD5,
    Content,
    NRESentimentScore,
    InsertedDate,
    AuthorSocialID,
    AuthorName,
    UserInfoScreenName,
    Bio,
    FollowersCount,
    FollowingCount,
    TweetCount,
    UserInfoIsVerified,
    PicUrl,
    AttachmentXML
FROM JOINED_ALERT_STREAM
WHERE 
    NumLikesCount > 10000 AND
    NumShareCount > 8000 OR
    NumCommentsCount >= 2000
EMIT CHANGES;
"""


# Invalid value NO_WINDOW for property WINDOW_TYPE: String must be one of: SESSION, HOPPING, TUMBLING, null",
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


joined_filterd_stream = """
CREATE TABLE koined_posts11 WITH (KEY_FORMAT='KAFKA',  TIMESTAMP='CreatedDate',TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss') AS 
SELECT 
    s.Composite_Key AS Composite_Key,
    LATEST_BY_OFFSET(s.BrandID) AS BrandID,
    LATEST_BY_OFFSET(s.BrandName) AS BrandName,
    LATEST_BY_OFFSET(s.CategoryGroupID) AS CategoryGroupID,
    LATEST_BY_OFFSET(s.CategoryID) AS CategoryID,
    LATEST_BY_OFFSET(s.CategoryName) AS CategoryName,
    LATEST_BY_OFFSET(s.ChannelType) AS ChannelType,
    LATEST_BY_OFFSET(s.ChannelGroupID) AS ChannelGroupID,
    LATEST_BY_OFFSET(s.Description) AS Description,
    LATEST_BY_OFFSET(s.SocialID) AS SocialID,
    LATEST_BY_OFFSET(s.CreatedDate) AS CreatedDate,
    LATEST_BY_OFFSET(s.SentimentType) AS SentimentType,
    LATEST_BY_OFFSET(s.PassivePositiveSentimentCount) AS PassivePositiveSentimentCount,
    LATEST_BY_OFFSET(s.NegativeSentimentCount) AS NegativeSentimentCount,
    LATEST_BY_OFFSET(s.NeutralSentimentCount) AS NeutralSentimentCount,
    LATEST_BY_OFFSET(s.Tagid) AS Tagid,
    LATEST_BY_OFFSET(s.UpperCategoryID) AS UpperCategoryID,
    LATEST_BY_OFFSET(s.IsDeleted) AS IsDeleted,
    LATEST_BY_OFFSET(s.SimplifiedText) AS SimplifiedText,
    LATEST_BY_OFFSET(s.Rating) AS Rating,
    LATEST_BY_OFFSET(s.IsVerified) AS IsVerified,
    LATEST_BY_OFFSET(s.RetweetedStatusID) AS RetweetedStatusID,
    LATEST_BY_OFFSET(s.InReplyToStatusId) AS InReplyToStatusId,
    LATEST_BY_OFFSET(s.MediaType) AS MediaType,
    COALESCE(LATEST_BY_OFFSET(a.NumLikesCount), LATEST_BY_OFFSET(s.NumLikesCount)) AS NumLikesCount,
    COALESCE(LATEST_BY_OFFSET(a.NumComments), LATEST_BY_OFFSET(s.NumComments)) AS NumComments,
    COALESCE(LATEST_BY_OFFSET(a.NumShareCount), LATEST_BY_OFFSET(s.NumShareCount)) AS NumShareCount,
    COALESCE(LATEST_BY_OFFSET(a.NumVideoViews), LATEST_BY_OFFSET(s.NumVideoViews)) AS NumVideoViews,
    LATEST_BY_OFFSET(a.Reach) AS Reach,
    COALESCE(LATEST_BY_OFFSET(a.Impression), LATEST_BY_OFFSET(s.Impression)) AS Impression,
    COALESCE(LATEST_BY_OFFSET(a.Engagement), LATEST_BY_OFFSET(s.Engagement)) AS Engagement,
    LATEST_BY_OFFSET(s.CategoryXML) AS CategoryXML,
    LATEST_BY_OFFSET(s.MediaEnum) AS MediaEnum,
    LATEST_BY_OFFSET(s.Lang) AS Language,
    LATEST_BY_OFFSET(s.LanguageName) AS Language_Name,
    LATEST_BY_OFFSET(s.PostType) AS PostType,
    LATEST_BY_OFFSET(s.IsBrandPost) AS IsBrandPost,
    LATEST_BY_OFFSET(s.InstagramPostType) AS InstagramPostType,
    LATEST_BY_OFFSET(s.SettingID) AS SettingID,
    LATEST_BY_OFFSET(s.quotedTweetCounts) AS quotedTweetCounts,
    LATEST_BY_OFFSET(s.InfluencerCategory) AS InfluencerCategory,
    LATEST_BY_OFFSET(s.TypeofComment) AS TypeofComment,
    LATEST_BY_OFFSET(s.OrderID) AS OrderID,
    LATEST_BY_OFFSET(s.IsHistoric) AS IsHistoric,
    LATEST_BY_OFFSET(s.MentionMD5) AS MentionMD5,
    LATEST_BY_OFFSET(s.Content) AS Content,
    LATEST_BY_OFFSET(s.NRESentimentScore) AS NRESentimentScore,
    LATEST_BY_OFFSET(s.InsertedDate) AS InsertedDate,
    LATEST_BY_OFFSET(s.AuthorSocialID) AS AuthorSocialID,
    LATEST_BY_OFFSET(s.AuthorName) AS AuthorName,
    LATEST_BY_OFFSET(s.UserInfoScreenName) AS UserInfoScreenName,
    LATEST_BY_OFFSET(s.Bio) AS Bio,
    LATEST_BY_OFFSET(s.FollowersCount) AS FollowersCount,
    LATEST_BY_OFFSET(s.FollowingCount) AS FollowingCount,
    LATEST_BY_OFFSET(s.TweetCount) AS TweetCount,
    LATEST_BY_OFFSET(s.UserInfoIsVerified) AS UserInfoIsVerified,
    LATEST_BY_OFFSET(s.PicUrl) AS PicUrl,
    LATEST_BY_OFFSET(s.AttachmentXML) AS AttachmentXML
FROM ALERT_FINAL_DATA_STREAM2 s
LEFT JOIN aggregated_table2 a
    ON s.Composite_Key = a.Composite_Key
EMIT CHANGES;

"""

joined_2 = """
CREATE TABLE koined_posts9 WITH (
    KEY_FORMAT = 'KAFKA',
    VALUE_FORMAT = 'JSON',
    TIMESTAMP = 'CreatedDate',
    TIMESTAMP_FORMAT = 'yyyy-MM-dd''T''HH:mm:ss'
) AS 
SELECT 
    s.Composite_Key AS Composite_Key,
    s.BrandID AS BrandID,
    s.BrandName AS BrandName,
    s.CategoryGroupID AS CategoryGroupID,
    s.CategoryID AS CategoryID,
    s.CategoryName AS CategoryName,
    s.ChannelType AS ChannelType,
    s.ChannelGroupID AS ChannelGroupID,
    s.Description AS Description,
    s.SocialID AS SocialID,
    s.CreatedDate AS CreatedDate,
    s.SentimentType AS SentimentType,
    s.PassivePositiveSentimentCount AS PassivePositiveSentimentCount,
    s.NegativeSentimentCount AS NegativeSentimentCount,
    s.NeutralSentimentCount AS NeutralSentimentCount,
    s.Tagid AS Tagid,
    s.UpperCategoryID AS UpperCategoryID,
    s.IsDeleted AS IsDeleted,
    s.SimplifiedText AS SimplifiedText,
    s.Rating AS Rating,
    s.IsVerified AS IsVerified,
    s.RetweetedStatusID AS RetweetedStatusID,
    s.InReplyToStatusId AS InReplyToStatusId,
    s.MediaType AS MediaType,
    COALESCE(a.NumLikesCount, s.NumLikesCount) AS NumLikesCount,
    COALESCE(a.NumComments, s.NumComments) AS NumComments,
    COALESCE(a.NumShareCount, s.NumShareCount) AS NumShareCount,
    COALESCE(a.NumVideoViews, s.NumVideoViews) AS NumVideoViews,
    a.Reach AS Reach,
    COALESCE(a.Impression, s.Impression) AS Impression,
    COALESCE(a.Engagement, s.Engagement) AS Engagement,
    s.CategoryXML AS CategoryXML,
    s.MediaEnum AS MediaEnum,
    s.Lang AS Language,
    s.LanguageName AS Language_Name,
    s.PostType AS PostType,
    s.IsBrandPost AS IsBrandPost,
    s.InstagramPostType AS InstagramPostType,
    s.SettingID AS SettingID,
    s.quotedTweetCounts AS quotedTweetCounts,
    s.InfluencerCategory AS InfluencerCategory,
    s.TypeofComment AS TypeofComment,
    s.OrderID AS OrderID,
    s.IsHistoric AS IsHistoric,
    s.MentionMD5 AS MentionMD5,
    s.Content AS Content,
    s.NRESentimentScore AS NRESentimentScore,
    s.InsertedDate AS InsertedDate,
    s.AuthorSocialID AS AuthorSocialID,
    s.AuthorName AS AuthorName,
    s.UserInfoScreenName AS UserInfoScreenName,
    s.Bio AS Bio,
    s.FollowersCount AS FollowersCount,
    s.FollowingCount AS FollowingCount,
    s.TweetCount AS TweetCount,
    s.UserInfoIsVerified AS UserInfoIsVerified,
    s.PicUrl AS PicUrl,
    s.AttachmentXML AS AttachmentXML
FROM ALERT_FINAL_DATA_STREAM2 s
LEFT JOIN aggregated_table2 a
    ON s.Composite_Key = a.Composite_Key
EMIT CHANGES;
"""

aggregated_table = """
CREATE TABLE aggregated_table2 WITH (KEY_FORMAT='KAFKA',  TIMESTAMP='CreatedDate',TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss') AS 
SELECT 
    Composite_Key PRIMARY KEY,
    LATEST_BY_OFFSET(NumComments) AS NumComments,
    LATEST_BY_OFFSET(NumLikesCount) AS NumLikesCount,
    LATEST_BY_OFFSET(NumShareCount) AS NumShareCount,
    LATEST_BY_OFFSET(NumVideoViews) AS NumVideoViews,
    LATEST_BY_OFFSET(Reach) AS Reach,
    LATEST_BY_OFFSET(Impression) AS Impression,
    LATEST_BY_OFFSET(Engagement) AS Engagement  
FROM 
    ALERT_UPDATED_DATA_STREAM
GROUP BY 
    Composite_Key;
"""

aggregated_2 = """
CREATE TABLE AGGREGATED_TABLE2 WITH  (KEY_FORMAT='KAFKA',  TIMESTAMP='CreatedDate',TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss') AS SELECT
  ALERT_UPDATED_DATA_STREAM.COMPOSITE_KEY COMPOSITE_KEY,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.NUMCOMMENTS) NUMCOMMENTS,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.NUMLIKESCOUNT) NUMLIKESCOUNT,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.NUMSHARECOUNT) NUMSHARECOUNT,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.NUMVIDEOVIEWS) NUMVIDEOVIEWS,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.REACH) REACH,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.IMPRESSION) IMPRESSION,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.ENGAGEMENT) ENGAGEMENT,
  LATEST_BY_OFFSET(ALERT_UPDATED_DATA_STREAM.CREATEDDATE) CREATEDDATE
FROM ALERT_UPDATED_DATA_STREAM ALERT_UPDATED_DATA_STREAM
GROUP BY ALERT_UPDATED_DATA_STREAM.COMPOSITE_KEY
EMIT CHANGES;
"""

creating_aggregated_table = """
CREATE TABLE FINAL_AGGREGATED_TABLE 
WITH (KAFKA_TOPIC='AggregatedFinalData', 
      KEY_FORMAT='KAFKA',
      TIMESTAMP='CreatedDate', 
      TIMESTAMP_FORMAT='yyyy-MM-dd''T''HH:mm:ss') AS
SELECT 
    CompositeKey,
    LATEST_BY_OFFSET(BrandID) AS BrandID,
    LATEST_BY_OFFSET(BrandName) AS BrandName,
    LATEST_BY_OFFSET(CategoryGroupID) AS CategoryGroupID,
    LATEST_BY_OFFSET(CategoryID) AS CategoryID,
    LATEST_BY_OFFSET(CategoryName) AS CategoryName,
    LATEST_BY_OFFSET(ChannelType) AS ChannelType,
    LATEST_BY_OFFSET(ChannelGroupID) AS ChannelGroupID,
    LATEST_BY_OFFSET(Description) AS Description,
    LATEST_BY_OFFSET(Caption) AS Caption,
    LATEST_BY_OFFSET(ScreenName) AS ScreenName,
    LATEST_BY_OFFSET(SocialID) AS SocialID,
    LATEST_BY_OFFSET(ObjectID) AS ObjectID,
    LATEST_BY_OFFSET(NumLikesORFollowers) AS NumLikesORFollowers,
    LATEST_BY_OFFSET(NumLikesCount) AS NumLikesCount,
    LATEST_BY_OFFSET(NumComments) AS NumComments,
    LATEST_BY_OFFSET(NumCommentsCount) AS NumCommentsCount,
    LATEST_BY_OFFSET(NumShareCount) AS NumShareCount,
    LATEST_BY_OFFSET(NumVideoViews) AS NumVideoViews,
    LATEST_BY_OFFSET(ShareCount) AS ShareCount,
    LATEST_BY_OFFSET(CreatedDate) AS CreatedDate,
    LATEST_BY_OFFSET(SentimentType) AS SentimentType,
    LATEST_BY_OFFSET(PassivePositiveSentimentCount) AS PassivePositiveSentimentCount,
    LATEST_BY_OFFSET(NegativeSentimentCount) AS NegativeSentimentCount,
    LATEST_BY_OFFSET(NeutralSentimentCount) AS NeutralSentimentCount,
    LATEST_BY_OFFSET(Tagid) AS Tagid,
    LATEST_BY_OFFSET(UpperCategoryID) AS UpperCategoryID,
    LATEST_BY_OFFSET(IsDeleted) AS IsDeleted,
    LATEST_BY_OFFSET(SimplifiedText) AS SimplifiedText,
    LATEST_BY_OFFSET(Rating) AS Rating,
    LATEST_BY_OFFSET(IsVerified) AS IsVerified,
    LATEST_BY_OFFSET(RetweetedStatusID) AS RetweetedStatusID,
    LATEST_BY_OFFSET(InReplyToStatusId) AS InReplyToStatusId,
    LATEST_BY_OFFSET(MediaType) AS MediaType,
    LATEST_BY_OFFSET(Reach) AS Reach,
    LATEST_BY_OFFSET(Impression) AS Impression,
    LATEST_BY_OFFSET(Engagement) AS Engagement,
    LATEST_BY_OFFSET(CategoryXML) AS CategoryXML,
    LATEST_BY_OFFSET(PostSocialID) AS PostSocialID,
    LATEST_BY_OFFSET(ParentSocialID) AS ParentSocialID,
    LATEST_BY_OFFSET(MediaEnum) AS MediaEnum,
    LATEST_BY_OFFSET(Lang) AS Lang,
    LATEST_BY_OFFSET(LanguageName) AS LanguageName,
    LATEST_BY_OFFSET(PostType) AS PostType,
    LATEST_BY_OFFSET(IsBrandPost) AS IsBrandPost,
    LATEST_BY_OFFSET(InstagramPostType) AS InstagramPostType,
    LATEST_BY_OFFSET(SettingID) AS SettingID,
    LATEST_BY_OFFSET(FilterKeywords) AS FilterKeywords,
    LATEST_BY_OFFSET(quotedTweetCounts) AS quotedTweetCounts,
    LATEST_BY_OFFSET(InfluencerCategory) AS InfluencerCategory,
    LATEST_BY_OFFSET(InfluencerCategoryID) AS InfluencerCategoryID,
    LATEST_BY_OFFSET(InfluencerCategoryName) AS InfluencerCategoryName,
    LATEST_BY_OFFSET(TypeofComment) AS TypeofComment,
    LATEST_BY_OFFSET(OrderID) AS OrderID,
    LATEST_BY_OFFSET(IsHistoric) AS IsHistoric,
    LATEST_BY_OFFSET(MentionMD5) AS MentionMD5,
    LATEST_BY_OFFSET(Content) AS Content,
    LATEST_BY_OFFSET(NRESentimentScore) AS NRESentimentScore,
    LATEST_BY_OFFSET(InsertedDate) AS InsertedDate,
    LATEST_BY_OFFSET(AuthorSocialID) AS AuthorSocialID,
    LATEST_BY_OFFSET(AuthorName) AS AuthorName,
    LATEST_BY_OFFSET(UserInfoScreenName) AS UserInfoScreenName,
    LATEST_BY_OFFSET(UserSentiment) AS UserSentiment,
    LATEST_BY_OFFSET(Bio) AS Bio,
    LATEST_BY_OFFSET(FollowersCount) AS FollowersCount,
    LATEST_BY_OFFSET(FollowingCount) AS FollowingCount,
    LATEST_BY_OFFSET(TweetCount) AS TweetCount,
    LATEST_BY_OFFSET(UserInfoIsVerified) AS UserInfoIsVerified,
    LATEST_BY_OFFSET(PicUrl) AS PicUrl,
    LATEST_BY_OFFSET(URL) AS URL,
    LATEST_BY_OFFSET(AttachmentXML) AS AttachmentXML,
    LATEST_BY_OFFSET(ConversationId) AS ConversationId,
    LATEST_BY_OFFSET(Title) AS Title
FROM ALERT_FINAL_DATA_STREAM
GROUP BY CompositeKey;
"""

if __name__ == "__main__":
    # Create the final_data_stream streamALERT_FINAL_DATA_STREAM
    # execute_ksqldb_query(create_final_data_stream_query)
    # execute_ksqldb_query(create_update_data_stream_query)
    # execute_ksqldb_query(aggregated_2)

    execute_ksqldb_query(creating_aggregated_table)
# CLEANUP_POLICY='delete', KAFKA_TOPIC='LATEST_ALERT_STREAM', PARTITIONS=10, REPLICAS=1, RETENTION_MS=604800000) AS
"""
[{'@type': 'currentStatus', 'statementText': "CREATE OR REPLACE STREAM JOINED_ALERT_STREAM WITH (CLEANUP_POLICY='delete', KAFKA_TOPIC='JOINED_ALERT_STREAM', PARTITIONS=10, REPLICAS=1, RETENTION_MS=604800000) AS SELECT\n  A.BRANDID BRANDID,\n  A.BRANDNAME BRANDNAME,\n  A.CATEGORYGROUPID CATEGORYGROUPID,\n  A.CATEGORYID CATEGORYID,\n  A.CATEGORYNAME CATEGORYNAME,\n  A.CHANNELTYPE CHANNELTYPE,\n  A.CHANNELGROUPID CHANNELGROUPID,\n  A.DESCRIPTION DESCRIPTION,\n  A.SOCIALID A_SOCIALID,\n  A.NUMLIKESORFOLLOWERS NUMLIKESORFOLLOWERS,\n  COALESCE(U.NUMLIKESCOUNT, A.NUMLIKESCOUNT) NUMLIKESCOUNT,\n  COALESCE(U.NUMCOMMENTS, A.NUMCOMMENTS) NUMCOMMENTS,\n  COALESCE(U.NUMCOMMENTSCOUNT, A.NUMCOMMENTSCOUNT) NUMCOMMENTSCOUNT,\n  COALESCE(U.NUMSHARECOUNT, A.NUMSHARECOUNT) NUMSHARECOUNT,\n  COALESCE(U.NUMVIDEOVIEWS, A.NUMVIDEOVIEWS) NUMVIDEOVIEWS,\n  A.SHARECOUNT SHARECOUNT,\n  A.CREATEDDATE A_CREATEDDATE,\n  A.SENTIMENTTYPE SENTIMENTTYPE,\n  A.PASSIVEPOSITIVESENTIMENTCOUNT PASSIVEPOSITIVESENTIMENTCOUNT,\n  A.NEGATIVESENTIMENTCOUNT NEGATIVESENTIMENTCOUNT,\n  A.NEUTRALSENTIMENTCOUNT NEUTRALSENTIMENTCOUNT,\n  A.TAGID TAGID,\n  A.UPPERCATEGORYID UPPERCATEGORYID,\n  A.ISDELETED ISDELETED,\n  A.SIMPLIFIEDTEXT SIMPLIFIEDTEXT,\n  A.RATING RATING,\n  A.ISVERIFIED ISVERIFIED,\n  A.RETWEETEDSTATUSID RETWEETEDSTATUSID,\n  A.INREPLYTOSTATUSID INREPLYTOSTATUSID,\n  A.MEDIATYPE MEDIATYPE,\n  A.REACH REACH,\n  A.IMPRESSION IMPRESSION,\n  A.ENGAGEMENT ENGAGEMENT,\n  A.CATEGORYXML CATEGORYXML,\n  A.MEDIAENUM MEDIAENUM,\n  A.LANG LANG,\n  A.LANGUAGENAME LANGUAGENAME,\n  A.POSTTYPE POSTTYPE,\n  A.ISBRANDPOST ISBRANDPOST,\n  A.INSTAGRAMPOSTTYPE INSTAGRAMPOSTTYPE,\n  A.SETTINGID SETTINGID,\n  A.QUOTEDTWEETCOUNTS QUOTEDTWEETCOUNTS,\n  A.INFLUENCERCATEGORY INFLUENCERCATEGORY,\n  A.TYPEOFCOMMENT TYPEOFCOMMENT,\n  A.ORDERID ORDERID,\n  A.ISHISTORIC ISHISTORIC,\n  A.MENTIONMD5 MENTIONMD5,\n  A.CONTENT CONTENT,\n  A.NRESENTIMENTSCORE NRESENTIMENTSCORE,\n  A.INSERTEDDATE INSERTEDDATE,\n  A.AUTHORSOCIALID AUTHORSOCIALID,\n  A.AUTHORNAME AUTHORNAME,\n  A.USERINFOSCREENNAME USERINFOSCREENNAME,\n  A.BIO BIO,\n  A.FOLLOWERSCOUNT FOLLOWERSCOUNT,\n  A.FOLLOWINGCOUNT FOLLOWINGCOUNT,\n  A.TWEETCOUNT TWEETCOUNT,\n  A.USERINFOISVERIFIED USERINFOISVERIFIED,\n  A.PICURL PICURL,\n  A.ATTACHMENTXML ATTACHMENTXML\nFROM ALERT_FINAL_DATA_STREAM A\nLEFT OUTER JOIN ALERT_UPDATED_DATA_STREAM U WITHIN 72 HOURS ON ((A.SOCIALID = U.SOCIALID))\nWHERE (A.CREATEDDATE = U.CREATEDDATE)\nEMIT CHANGES;", 'commandId': 'stream/`JOINED_ALERT_STREAM`/create', 'commandStatus': {'status': 'SUCCESS', 'message': 'Created query with ID CSAS_JOINED_ALERT_STREAM_19', 'queryId': 'CSAS_JOINED_ALERT_STREAM_19'}, 'commandSequenceNumber': 20, 'warnings': [{'message': 'DEPRECATION NOTICE: Stream-stream joins statements without a GRACE PERIOD will not be accepted in a future ksqlDB version.\nPlease use the GRACE PERIOD clause as specified in https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-push-query/'}]}]
"""
