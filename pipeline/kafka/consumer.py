from kafka import KafkaConsumer
from json import loads
from time import sleep

consumer = KafkaConsumer(
    'gemini-feed',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

count = 0
for event in consumer:
    if count<=100:
        event_data = event.value
        print('Receiving...\n',event_data)
        sleep(0.5)
    else:
        break
    count+=1
