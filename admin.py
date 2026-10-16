from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

# Connect to Kafka through the Docker network
admin = AdminClient({"bootstrap.servers": "kafka:19092"})
topic = "wikistreams"

try:
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        request_timeout=10
    )
    futures[topic].result(timeout=15)
    print(f"Topic '{topic}' created successfully.")

except KafkaException as error:
    if error.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
        print(f"Topic '{topic}' already exists.")
    else:
        raise