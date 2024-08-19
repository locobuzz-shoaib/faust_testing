import requests
import json

ksql_url = "http://192.168.0.107:8088/query"
ksql_query = {
    "ksql": """SELECT TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd HH:mm:ss', 'Asia/Kolkata') AS readable_timestamp,
    compositekey 
    FROM FINAL_AGGREGATED_TABLE 
    WHERE numcommentscount > 5000 AND numlikescount > 8000 
    HAVING COUNT(*) >= 11 
    EMIT CHANGES;""",
    "streamsProperties": {
        "processing.guarantee": "exactly_once"
    }
}

response = requests.post(ksql_url, headers={'Content-Type': 'application/vnd.ksql.v1+json'}, data=json.dumps(ksql_query), stream=True)

if response.status_code == 200:
    for line in response.iter_lines():
        if line:
            decoded_line = json.loads(line.decode('utf-8'))
            print(decoded_line)
        else:
            print("No data received")
    print("Query executed successfully")
else:
    print(f"Error: {response.status_code}, {response.text}")
