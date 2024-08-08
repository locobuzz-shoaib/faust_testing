import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://localhost:8088"

# ksqlDB query to create the mentions_stream_2 stream
create_mentions_stream_query = """
CREATE STREAM final_data_stream (
    tagid STRING key,
    mentionid STRING,
    createddate BIGINT key,
    uniqueid STRING key,
    categoryid STRING key,
    brandid STRING key,
    socialid STRING,
    postsocialid STRING,
    channel STRING,
    post_type STRING,
    sentiment STRING,
    description STRING
) WITH (
    KAFKA_TOPIC='finaldata',
    VALUE_FORMAT='JSON'
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
    # Create the mentions_stream_2 stream
    execute_ksqldb_query(create_mentions_stream_query)
