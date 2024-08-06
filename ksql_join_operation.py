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
    description STRING
) WITH (KAFKA_TOPIC='updateddata', VALUE_FORMAT='JSON');
"""

# ksqlDB query to perform join with WITHIN clause
create_joined_stream_query = """
CREATE STREAM joined_stream AS
SELECT
    s1.mentionid AS mentionid_one,
    s1.uniqueid AS uniqueid_one,
    s1.createdate AS createdate_one,
    s1.numlikes AS numlikes_one,
    s1.numcomments AS numcomments_one,
    s1.numshare AS numshare_one,
    s1.socialid AS socialid_one,
    s1.postsocialid AS postsocialid_one,
    s1.description AS description_one,
    s2.mentionid AS mentionid_two,
    s2.uniqueid AS uniqueid_two,
    s2.createdate AS createdate_two,
    s2.numlikes AS numlikes_two,
    s2.numcomments AS numcomments_two,
    s2.numshare AS numshare_two,
    s2.socialid AS socialid_two,
    s2.postsocialid AS postsocialid_two,
    s2.description AS description_two
FROM stream_one s1
JOIN stream_two s2
  WITHIN 24 HOUR
  ON s1.socialid = s2.postsocialid
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

    # Perform the join
    execute_ksqldb_query(create_joined_stream_query)
