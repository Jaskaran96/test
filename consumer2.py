import redis
import multiprocessing
import glob
from csv import reader
from constants import LOGFILE, N_WORKERS, DATA_PATH,IN,FNAME
sleep_ms = 500

def process_tweet(tweet):
    # Replace this with your actual tweet processing logic
    print("Processing tweet:", tweet)
# resp = r.xreadgroup(group_name, consumer_name, {stream_name: '>'}, count=1, block=sleep_ms)
def worker(group_name, consumer_name, stream_name):
    r = redis.Redis(host='localhost', port=6379, password="pass",db=0,decode_responses=False)
    print("going in ")
    count = 0
    last_id = 0
    while True:
        try:
            resp = r.xreadgroup(group_name,consumer_name,{stream_name : '>'},count=1, block=sleep_ms)
            if resp:
                key, messages = resp[0]
                last_id, data = messages[0]
                # print("REDIS ID: ", last_id)
                # print("      --> ", data)
                count+=1
                r.xack(stream_name, group_name, last_id)
            else:
                break
        except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))
    print(f"For {consumer_name} printed {count} rows")

if __name__ == '__main__':
    
    stream_name = 'newtest'
    group_name = 'worker'
    consumer_name = ['consumer1','consumer2','consumer3','consumer4']
    num_workers = 4

    def load_csv(filename):
        # Open file in read mode
        file = open(filename,"r")
        # Reading file 
        lines = reader(file)
        
        # Converting into a list 
        data = list(lines)
        return data
    rds = redis.Redis(host='localhost', port=6379, password="pass",db=0,decode_responses=False)
    rds.flushall()
    rds.xgroup_create(IN, "worker", id="0", mkstream=True)
    for file in glob.glob(DATA_PATH):
        data = load_csv(file)
        print(f"{len(data)}")
        
        # Let's print the first 5 datapoints
        for row in data:
            rds.xadd(IN, {FNAME: row[4]})

    # Create a multiprocessing event to signal worker termination
    stop_event = multiprocessing.Event()

    # Create and start worker processes
    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker, args=(group_name, consumer_name[i], stream_name))
        processes.append(p)
        p.start()

    try:
        # Wait for all worker processes to complete
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("Terminating workers...")
        stop_event.set()
        for p in processes:
            p.join()

    print("All workers have finished.")