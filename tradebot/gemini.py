import faust
import json
import itertools
import operator
from kafka import KafkaProducer
import time

faust_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def handler(record):
    output = {}
    output['type'] = record[0]
    output['value'] = record[1]
    output['timestamp'] = int(time.time())
    faust_producer.send('faust.out', bytes(json.dumps(output), 'utf-8'))
    faust_producer.flush()

def price_volume(x):
    price = float(x['price'])
    remaining = float(x['remaining'])
    x['price_volume'] = price * remaining
    return (x['side'], x['price_volume'])

def accumulate(l):
    it = itertools.groupby(l, operator.itemgetter(0))
    for key, subiter in it:
       yield key, sum(item[1] for item in subiter) 

app = faust.App('gemini', broker='kafka://localhost:9092')

topic = app.topic('gemini-feed', value_type=bytes)
@app.agent(topic)
async def gemini_feed_process(stream):
    # print("I AM HERE")
    async for messages in stream.take(1e6, within=60):
        parsed_pv = list(map(lambda x: price_volume(x), messages)) #map object: convert to list using list()
        # print(parsed_pv[0])
        grouped_sorted = list(accumulate(sorted(parsed_pv)))
        # print(grouped_sorted)
        for rec in grouped_sorted:
            handler(rec)
        bid_to_ask_ratio = ("price_volume", grouped_sorted[1][1]/grouped_sorted[0][1])
        # print(bid_to_ask_ratio)
        handler(bid_to_ask_ratio)

@app.timer(60.0)
async def my_periodic_task():
    print('SIXTY SECONDS PASSED')

if __name__ == '__main__':
    app.main()
