import requests
import json
import time
import sys

ksql_url = "http://192.168.0.107:8088/query"
ksql_query = {
    "ksql": """SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss', 'Asia/Kolkata') AS readable_timestamp,
    compositekey 
    FROM FINAL_AGGREGATED_TABLE 
    WHERE numcommentscount > 5000 AND numlikescount > 8000 
    HAVING COUNT(*) >= 11 
    EMIT CHANGES;""",
    "streamsProperties": {

    }
}

response = requests.post(ksql_url, headers={'Content-Type': 'application/vnd.ksql.v1+json'},
                         data=json.dumps(ksql_query), stream=True)

# Define a timeout period in seconds
timeout_period = 10  # Timeout after 10 seconds of inactivity
last_received_time = time.time()

if response.status_code == 200:
    while True:
        current_time = time.time()

        # Check if the timeout period has been exceeded
        if current_time - last_received_time > timeout_period:
            print("No data received for the last 10 seconds, stopping the query.")
            response.close()  # Close the response
            sys.exit("Query finished due to inactivity.")

        try:
            line = next(response.iter_lines(), None)
            if line:
                decoded_line = json.loads(line.decode('utf-8'))
                print(decoded_line)
                last_received_time = time.time()  # Reset the timeout timer after receiving data
            else:
                time.sleep(1)  # Sleep briefly to avoid tight loop if no data is received
        except StopIteration:
            print("End of data stream.")
            break
        except requests.exceptions.Timeout:
            print("Request timed out. Continuing...")
            continue

    response.close()
    sys.exit("Query finished due to inactivity.")

else:
    print(f"Error: {response.status_code}, {response.text}")
    sys.exit("Failed to execute query.")
