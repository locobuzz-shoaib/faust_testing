import redis

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0)


def process_and_count_unique(record):
    # Add the record to the HyperLogLog
    r.pfadd("my_hyperloglog111", record)

    # Retrieve the approximate unique count
    unique_count = r.pfcount("my_hyperloglog1112")

    print(f"Current unique count: {unique_count}")


# Example usage
record = "record_12345"
process_and_count_unique(record)
