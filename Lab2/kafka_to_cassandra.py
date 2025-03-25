from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import uuid

consumer = KafkaConsumer(
    'aggregated_data',
    bootstrap_servers='localhost:9092',
    group_id='cassandra-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

cluster = Cluster(['localhost'])
session = cluster.connect('lab')

for message in consumer:
    data = message.value
    country = data['country']
    user_id = uuid.uuid4()
    name = data['name']
    email = data['email']

    session.execute(
        """
        INSERT INTO user_data (country, user_id, name, email)
        VALUES (%s, %s, %s, %s)
        """,
        (country, user_id, name, email)
    )

    print(f"Inserted data: {data}")

consumer.close()
cluster.shutdown()