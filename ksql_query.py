import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://172.18.244.10:8088"

# ksqlDB query to fetch data from the KTable
ksql_query = {
    "ksql": "SELECT * FROM parent_post_aggregates;",
    "streamsProperties": {}
}


# Function to query ksqlDB and fetch results
def query_ksqldb(ksql_query):
    response = requests.post(f"{KSQLDB_SERVER_URL}/query", json=ksql_query)
    if response.status_code == 200:
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                # Print or process each line of the result
                print(decoded_line)
    else:
        print(f"Failed to query ksqlDB: {response.status_code}, {response.text}")


if __name__ == "__main__":
    query_ksqldb(ksql_query)
