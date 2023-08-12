import glob
import logging
import os
import signal
import sys
import json
from threading import current_thread
from csv import reader
import time
from constants import LOGFILE, N_WORKERS, DATA_PATH,DATA_PATH2,COUNT
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
  # Clear the log file
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
    print(f"file is {file}")
    rds.add_file(file)
  
  signal.signal(signal.SIGTERM, sigterm_handler)
  for i in range(N_WORKERS):
    workers.append(WcWorker(rds=rds))

  for w in workers:
    w.create_and_run(rds=rds)

  logging.debug('Created all the workers')

  while True:
    print("ENTERNING")
    try:
      pid_killed, status = os.wait()
      logging.info(f"Worker-{pid_killed} died with status {status}!")
    except:
      print("HERE")
      break

  localdic = {}
  for file in glob.glob(DATA_PATH2):
    f = open(file)
    data = json.load(f)
    print(f"Loading file {file}")
    for word,count in data.items():
      try : 
        localdic[word]+=count
      except:
        localdic[word]=count
    print(f"DONe file {file}")
    

  print(len(localdic))
  rx = time.time()
  x = 0
  l1={}

  for word,count in localdic.items():
    l1[word]=count
    x+=1
    if(x==100000):
      x=0
      rds.rds.zadd(COUNT,l1)
      l1={}
  print(f"x is {x}")
  rds.rds.zadd(COUNT,l1)
  ry = time.time()
  print(f"completed in ${ry-rx}")
  #[(b'to', 1822200.0), (b'the', 1568100.0), (b'you', 1078555.0), (b'a', 936660.0), (b'I', 921800.0)]
  print(rds.top(5))
  y = time.time()
  print(f"********************* {y-xff}")
  for file in glob.glob(DATA_PATH2):
    os.remove(file)

  # for word, c in rds.top(3):
  #   logging.info(f"{word.decode()}: {c}")