from kafka import KafkaConsumer, KafkaProducer
from const import *
import sys

try:
    topic_in  = sys.argv[1]  # topic1: onde consome
    topic_out = sys.argv[2]  # topic2: onde publica
except:
    print('Usage: python3 consumer_producer.py <topic_in> <topic_out>')
    exit(1)
consumer = KafkaConsumer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)
consumer.subscribe([topic_in])

producer = KafkaProducer(
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT]
)

print(f'[*] Consumindo de "{topic_in}" e publicando em "{topic_out}"...')

for msg in consumer:
    received = msg.value.decode()
    print(f'[RECEBIDO]  {received}')

    processed = received.upper() + ' [PROCESSED]'

    producer.send(topic_out, value=processed.encode())
    producer.flush()
    print(f'[PUBLICADO] {processed}')