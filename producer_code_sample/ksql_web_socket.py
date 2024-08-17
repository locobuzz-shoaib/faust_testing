import requests
import json

ksql_url = "http://192.168.0.107:8088/query"
ksql_query = {
    "ksql": "SELECT * FROM HIGH_ENGAGEMENT_TBALE WHERE NUMCOMMENTSCOUNT > 10000 AND NUMLIKESCOUNT > 20000 AND CREATEDDATE > '2024-08-08';",
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
    print(f"Error: {response.status_code}, {response.text}")
