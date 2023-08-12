from redis.client import Redis

rds = Redis(host='localhost', port=6379, password='pass',
                       db=0, decode_responses=False)

last_id = 0
sleep_ms = 2000
STREAM_NAME = "newtest"
while True:
    try:
        resp = rds.xread(
            {STREAM_NAME: last_id}, count=1, block=sleep_ms
        )
        if resp:
            key, messages = resp[0]
            last_id, data = messages[0]
            print("REDIS ID: ", last_id)
            print("      --> ", data)
        else:
            break

    except ConnectionError as e:
        print("ERROR REDIS CONNECTION: {}".format(e))