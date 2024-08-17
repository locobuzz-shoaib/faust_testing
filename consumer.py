from confluent_kafka import Consumer, KafkaError, KafkaException

# Configuration for Kafka consumer
conf = {
    'bootstrap.servers': 'b-2.locobuzzuatmskcluster.4psd4m.c3.kafka.ap-south-1.amazonaws.com:9092,b-1.locobuzzuatmskcluster.4psd4m.c3.kafka.ap-south-1.amazonaws.com:9092',  # replace with your Kafka broker(s)
    'group.id': 'my-consumer-group',  # replace with your consumer group ID
    'auto.offset.reset': 'earliest'  # start from the earliest message if no offset is found
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic = 'AlertFinalData'  # replace with your Kafka topic
consumer.subscribe([topic])


def consume_loop(consumer, topics):
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # timeout in seconds

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of partition {msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"Consumed message from topic {msg.key()}: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == '__main__':
    consume_loop(consumer, [topic])
