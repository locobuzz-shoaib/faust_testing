import requests

# ksqlDB server URL
ksqlDB_url = 'http://192.168.0.107:8088/query'

# Define the ksql query
ksql_query = {
    "ksql": """
    SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss', 'Asia/Kolkata') AS readable_timestamp,
    compositekey 
    FROM FINAL_AGGREGATED_TABLE 
    WHERE numcommentscount > 5000 AND numlikescount > 8000 
    HAVING COUNT(*) >= 11 
    EMIT CHANGES;
    """,
    "streamsProperties": {}
}

# Set a timeout for the request
timeout_seconds = 10  # Adjust the timeout as needed

try:
    # Send the query to ksqlDB
    response = requests.post(ksqlDB_url, json=ksql_query, timeout=30)

    # Check if the request was successful
    if response.status_code == 200:
        for line in response.iter_lines():
            if line:
                print(line.decode('utf-8'))
    else:
        print(f"Failed to execute query: {response.status_code} {response.text}")

except requests.Timeout:
    print("The request timed out.")

except requests.RequestException as e:
    print(f"An error occurred: {e}")
