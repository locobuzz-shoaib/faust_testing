import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://localhost:8088"

# ksqlDB queries to create streams
create_stream_one_query = """
CREATE STREAM stream_one (
    mentionid STRING,
    uniqueid STRING,
    createdate STRING,
    numlikes INT,
    numcomments INT,
    numshare INT,
    socialid STRING,
    postsocialid STRING,
    brandid STRING,
    categoryid STRING,
    description STRING
) WITH (KAFKA_TOPIC='finaldata', VALUE_FORMAT='JSON');
"""

create_stream_two_query = """
CREATE STREAM stream_two (
    mentionid STRING,
    uniqueid STRING,
    createdate STRING,
    numlikes INT,
    numcomments INT,
    numshare INT,
    socialid STRING,
    postsocialid STRING,
    brandid STRING,
    categoryid STRING,
    description STRING
) WITH (KAFKA_TOPIC='updateddata', VALUE_FORMAT='JSON');
"""

create_stream_three_query = """
CREATE STREAM stream_three (
    mentionid STRING,
    uniqueid STRING,
    createdate STRING,
    socialid STRING,
    postsocialid STRING
) WITH (KAFKA_TOPIC='additionaldata', VALUE_FORMAT='JSON');
"""

# ksqlDB query to perform complex join with LIKE condition
create_complex_joined_stream_query = """
CREATE STREAM complex_joined_stream AS
SELECT
    s1.mentionid AS mentionid_one,
    s1.uniqueid AS uniqueid_one,
    s1.createdate AS createdate_one,
    s1.numlikes AS numlikes_one,
    s1.numcomments AS numcomments_one,
    s1.numshare AS numshare_one,
    s1.socialid AS socialid_one,
    s1.postsocialid AS postsocialid_one,
    s1.brandid AS brandid_one,
    s1.categoryid AS categoryid_one,
    s1.description AS description_one,
    s2.mentionid AS mentionid_two,
    s2.uniqueid AS uniqueid_two,
    s2.createdate AS createdate_two,
    s2.numlikes AS numlikes_two,
    s2.numcomments AS numcomments_two,
    s2.numshare AS numshare_two,
    s2.socialid AS socialid_two,
    s2.postsocialid AS postsocialid_two,
    s2.brandid AS brandid_two,
    s2.categoryid AS categoryid_two,
    s2.description AS description_two,
    s3.createdate AS createdate_three
FROM stream_one s1
LEFT JOIN stream_two s2
  WITHIN 24 HOURS
  ON s1.socialid = s2.postsocialid
LEFT JOIN stream_three s3
  WITHIN 24 HOURS
  ON s1.mentionid = s3.mentionid
WHERE s1.numlikes > 100
  AND s2.numcomments < 50
  AND (s1.description LIKE '%shoaib%' AND s1.description LIKE '%atharva%' OR s1.description LIKE '%rahul%')
EMIT CHANGES;
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
    # Create the first stream
    execute_ksqldb_query(create_stream_one_query)

    # Create the second stream
    execute_ksqldb_query(create_stream_two_query)

    # Create the third stream
    execute_ksqldb_query(create_stream_three_query)

    # Perform the complex join
    execute_ksqldb_query(create_complex_joined_stream_query)
