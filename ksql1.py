import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://localhost:8088"

# ksqlDB query to create the final_data_stream stream with composite keys
create_final_data_stream_query = """
CREATE STREAM final_data_stream (
    tagid STRING,
    mentionid STRING,
    createddate BIGINT,
    uniqueid STRING,
    categoryid STRING,
    brandid STRING,
    socialid STRING,
    postsocialid STRING,
    channel STRING,
    post_type STRING,
    sentiment STRING,
    description STRING,
    PRIMARY KEY (tagid, createddate, uniqueid, categoryid, brandid)
) WITH (
    KAFKA_TOPIC='finaldata',
    VALUE_FORMAT='JSON'
);
"""
1
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
