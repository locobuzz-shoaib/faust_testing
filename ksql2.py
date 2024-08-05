import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://172.18.244.10:8088"

# ksqlDB statement to create the stream
create_stream_statement = """
CREATE STREAM finaldata_stream (
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

# ksqlDB statement to create the table with the required aggregations
create_table_statement = """
CREATE TABLE parent_post_aggregates4 AS
SELECT
    socialid,
    COUNT(*) AS comment_count,
    MAX(numlikes) AS like_count,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end
FROM finaldata_stream
WINDOW TUMBLING (SIZE 1 MINUTES)
WHERE postsocialid IS NULL
GROUP BY socialid
HAVING MAX(numlikes) > 200  AND COUNT(*) > 1;
"""

drop_statements = [
    "DROP TABLE parent_post_aggregates2 IF EXISTS CASCADE;",
    "DROP TABLE parent_post_aggregates3 IF EXISTS CASCADE;",
    "DROP TABLE parent_post_aggregates4 IF EXISTS CASCADE;",
    "DROP STREAM finaldata_stream IF EXISTS CASCADE;"
]


# Function to send ksqlDB statements
def send_ksqldb_statement(statement):
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": statement}
    )
    if response.status_code == 200:
        print(f"Statement executed successfully: {statement}")
        print(response.json())
    else:
        print(f"Failed to execute statement: {response.status_code}, {response.text}")


if __name__ == "__main__":
    # Create the stream
    # send_ksqldb_statement(create_stream_statement)

    # Create the table
    for statement in drop_statements:
        send_ksqldb_statement(statement)
    # send_ksqldb_statement(drop_statements)
