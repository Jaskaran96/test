import sys
import asyncio
import time
import random
import uvloop
from redis.client import Redis

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()

async def process_message(redis, loop, group, consumer, streams):
    start = time.time()
    records = 0
    while True:
        result = await redis.xreadgroup(group, consumer, streams, count=1, latest_ids=['>'])
        if result:
            records += 1
            print(f"processing {result}")
            time.sleep(1)
        else:
            print("Timeout")
            break
    end = time.time()
    print(f"Reading {records} records took {end - start} seconds")

async def main(consumer):
    redis = Redis(host='localhost', port=6379, password='pass',db=0, decode_responses=False)
    streams = ['newtest']
    group = 'worker'

    await process_message(redis, loop, group, consumer, streams)
    redis.close()
    await redis.wait_closed()

if len(sys.argv) < 2:
    print("Consumer name is required")
    exit(1)

consumer = sys.argv[1]
loop.run_until_complete(main(consumer))