from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for j in range(50):
    data = {'counter': j}
    print('Sending...\n', data)
    producer.send('topic_test', value=data)
    sleep(2.5)
