"# faust_testing" 
```json
[{'@type': 'currentStatus', 'statementText': "CREATE STREAM FINAL_DATA_STREAM (MENTION_ID STRING, SENTIMENT STRING, CHANNEL STRING, MENTION_TIME TIMESTAMP) WITH (CLEANUP_POLICY='delete', KAFKA_TOPIC='finaldata', KEY_FORMAT='KAFKA', TIMESTAMP='mention_time', VALUE_FORMAT='JSON');", 'commandId': 'stream/`FINAL_DATA_STREAM`/create', 'commandStatus': {'status': 'SUCCESS', 'message': 'Stream created', 'queryId': None}, 'commandSequenceNumber': 2, 'warnings': []}]
```s