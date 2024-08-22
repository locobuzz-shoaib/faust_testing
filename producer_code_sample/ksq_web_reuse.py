import requests
import json
import time
import sys

ksql_url = "http://k8s-stagingl-ksqldbse-f8842d2fe8-49766aa07935ea85.elb.ap-south-1.amazonaws.com/query"
# query = "SELECT *\n                        FROM FINAL_AGGREGATED_TABLE \n                        WHERE BRANDID = 12168 AND CATEGORYID = 1808\n                AND (ChannelType IN (18,19) AND SimplifiedText LIKE ('% locobuzztest %')) AND NumShareCount = 1 AND NumCommentsCount = 2 AND NumLikesCount = 2 AND NumVideoViews = 2 AND Engagement = 5\n                AND  STRINGTOTIMESTAMP(createddate, 'yyyy-MM-dd''T''HH:mm:ss')  >= \n                STRINGTOTIMESTAMP('2024-08-20T15:04:23', 'yyyy-MM-dd''T''HH:mm:ss');"
query="""
SELECT *\n                        FROM FINAL_AGGREGATED_TABLE \n                        WHERE BRANDID = 12168 AND CATEGORYID = 1808\n                AND ChannelType IN (18,19) AND (CASE WHEN SimplifiedText LIKE '% locobuzztest %' THEN 1 ELSE 0 END=1) AND NumShareCount = 1 AND NumCommentsCount = 2 AND NumLikesCount = 2 AND NumVideoViews = 2 AND Engagement = 5\n                AND  STRINGTOTIMESTAMP(createddate, 'yyyy-MM-dd''T''HH:mm:ss')  >= \n                STRINGTOTIMESTAMP('2024-08-20T15:21:38', 'yyyy-MM-dd''T''HH:mm:ss');

"""
ksql_query = {
    "ksql": f'{query}',
    "streamsProperties": {}
}

response = requests.post(ksql_url, headers={'Content-Type': 'application/vnd.ksql.v1+json'},

                         data=json.dumps(ksql_query), stream=True)

# Define a timeout period in seconds
timeout_period = 10  # Timeout after 10 seconds of inactivity
last_received_time = time.time()
record_count = 0  # Counter for the number of records processed
max_records = 90  # Maximum number of records to process

if response.status_code == 200:
    try:
        for line in response.iter_lines():
            current_time = time.time()

            # Check if the timeout period has been exceeded
            if current_time - last_received_time > timeout_period:
                print("No data received for the last 10 seconds, stopping the query.")
                break

            if line:
                decoded_line = json.loads(line.decode('utf-8'))
                # check if the instance of the dictionary then its the information and if list it is the row with columns selected
                print("SS",decoded_line)
                last_received_time = time.time()  # Reset the timeout timer after receiving data
                record_count += 1

                # Check if the maximum number of records has been reached
                if record_count >= max_records:
                    print(f"Processed {max_records} records, stopping the query.")
                    break

            else:
                time.sleep(1)  # Sleep briefly to avoid tight loop if no data is received

    except requests.exceptions.Timeout:
        print("Request timed out. Exiting...")

    finally:
        response.close()
        # sys.exit("Query finished.")

else:
    print(f"Error: {response.status_code}, {response.text}")
    sys.exit("Failed to execute query.")
