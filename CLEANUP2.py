import requests

# ksqlDB server URL
KSQLDB_SERVER_URL = "http://172.18.244.10:8088"


# Function to list running queries
def list_queries():
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": "SHOW QUERIES;"}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list queries: {response.status_code}, {response.text}")
        return None


# Function to terminate a query
def terminate_query(query_id):
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": f"TERMINATE {query_id};"}
    )
    if response.status_code == 200:
        print(f"Query {query_id} terminated successfully.")
    else:
        print(f"Failed to terminate query {query_id}: {response.status_code}, {response.text}")


# Function to list streams and tables
def list_ksql_entities(entity_type):
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": f"SHOW {entity_type};"}
    )
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list {entity_type}: {response.status_code}, {response.text}")
        return None


# Function to drop a ksqlDB entity
def drop_ksql_entity(entity_type, entity_name):
    response = requests.post(
        f"{KSQLDB_SERVER_URL}/ksql",
        json={"ksql": f"DROP {entity_type} {entity_name};"}
    )
    if response.status_code == 200:
        print(f"{entity_type} {entity_name} dropped successfully.")
    else:
        print(f"Failed to drop {entity_type} {entity_name}: {response.status_code}, {response.text}")


if __name__ == "__main__":
    # List running queries
    queries_response = list_queries()
    queries = queries_response[0]['queries'] if queries_response else []

    # Terminate queries that depend on the tables
    tables_to_drop = ["parent_post_aggregates2", "parent_post_aggregates3", "parent_post_aggregates4"]
    for query in queries:
        for table in tables_to_drop:
            if table in query['sinks']:
                terminate_query(query['id'])

    # List tables and streams
    tables_response = list_ksql_entities("TABLES")
    streams_response = list_ksql_entities("STREAMS")

    tables = tables_response[0]['tables'] if tables_response else []
    streams = streams_response[0]['streams'] if streams_response else []

    # Drop tables if they exist
    for table in tables_to_drop:
        if any(entity['name'] == table for entity in tables):
            drop_ksql_entity("TABLE", table)

    # Drop stream if it exists
    stream_to_drop = "finaldata_stream"
    if any(entity['name'] == stream_to_drop for entity in streams):
        drop_ksql_entity("STREAM", stream_to_drop)
