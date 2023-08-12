import glob
import logging
import os
import signal
import sys
import json
from threading import current_thread
import time
from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT
from worker import WcWorker
from mrds import MyRedis

workers = []
xff = time.time()
def sigterm_handler(signum, frame):
  logging.info('Killing main process!')
  for w in workers:
    w.kill()
  sys.exit()

if __name__ == "__main__":
  LOCAL_DIR = "/home/baadalvm/test/output/*"
  # Clear the log file
  os.mkdir("output")
  open(LOGFILE, 'w').close()
  logging.basicConfig(# filename=LOGFILE,
                      level=logging.DEBUG,
                      force=True,
                      format='%(asctime)s [%(threadName)s] %(levelname)s: %(message)s')
  thread = current_thread()
  thread.name = "client"
  logging.debug('Done setting up loggers.')

  rds = MyRedis()

  for file in glob.glob(DATA_PATH):
    rds.add_file(file)
  
  signal.signal(signal.SIGTERM, sigterm_handler)
  for i in range(N_WORKERS):
    workers.append(WcWorker(rds=rds))

  for w in workers:
    w.create_and_run(rds=rds)

  logging.debug('Created all the workers')

  while True:
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      break

  localdic = {}
  mmx = time.time()
  for file in glob.glob(LOCAL_DIR):
    f = open(file)
    data = json.load(f)
    print(f"Loading file {file}")
    for word,count in data.items():
      try : 
        localdic[word]+=count
      except:
        localdic[word]=count
    print(f"Done file {file}")
  mmy = time.time()
  print(f"Completed reading all the JSON files in {mmy-mmx} seconds")
  print("Starting ingestion into Redis")
  rx = time.time()
  x = 0
  l1={}
  print(len(localdic.keys()))
  for word,count in localdic.items():
    l1[word]=count
    x+=1
    if(x==100000):
      x=0
      rds.rds.zadd(COUNT,l1)
      l1={}
  rds.rds.zadd(COUNT,l1)
  ry = time.time()
  print(f"Completed ingestion in Redis in {ry-rx} seconds")
  # #[(b'to', 1822200.0), (b'the', 1568100.0), (b'you', 1078555.0), (b'a', 936660.0), (b'I', 921800.0)]
  y = time.time()
  print(f"********************* Total work done in : {y-xff} seconds")
  for file in glob.glob(LOCAL_DIR):
    os.remove(file)
  os.rmdir("output")

  for word, c in rds.top(3):
    logging.info(f"{word.decode()}: {c}")
