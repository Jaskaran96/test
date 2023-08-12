import glob
import logging
import os
import signal
import sys
import json
import pathlib

from threading import current_thread
import time
from constants import LOGFILE, N_WORKERS, DATA_PATH,COUNT
from worker import WcWorker
from mrds import MyRedis

workers = []
LOCAL_DIR = ""
BATCH_COUNT = 100000
OUTPUT_FOLDER = "output"
LOCAL_DIR = ""
OUTPUT_DIR = ""

def sigterm_handler(signum, frame):
  logging.info('Killing main process!')
  for w in workers:
    w.kill()
  sys.exit()

def readData(localdic):
  mmx = time.time()
  for file in glob.glob(f"{OUTPUT_DIR}/*"):
    curFile = open(file)
    data = json.load(curFile)
    for word,count in data.items():
      try : 
        localdic[word]+=count
      except:
        localdic[word]=count
    print(f"Done file {file}")
  mmy = time.time()
  print(f"Completed reading all the files in {mmy-mmx} seconds")
  print(len(localdic.keys()))

def ingestRedis(localdic):
  print("Starting ingestion into Redis")
  rx = time.time()
  batch = 0
  tempDict={}
  for word,count in localdic.items():
    tempDict[word]=count
    batch+=1
    if(batch==BATCH_COUNT):
      batch=0
      rds.rds.zadd(COUNT,tempDict)
      tempDict={}
  rds.rds.zadd(COUNT,tempDict)
  ry = time.time()
  print(f"Completed ingestion in Redis in {ry-rx} seconds")

if __name__ == "__main__":
  LOCAL_DIR = pathlib.Path(__file__).parent.resolve()
  OUTPUT_DIR = os.path.join(LOCAL_DIR, OUTPUT_FOLDER)
  try : 
    startTime = time.time()
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
      print(file)
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
    readData(localdic)
    ingestRedis(localdic)

    endTime = time.time()
    print(f"********************* Total work done in : {endTime-startTime} seconds")

    for file in glob.glob(f"{OUTPUT_DIR}/*"):
      os.remove(file)

    os.rmdir("output")

    for word, c in rds.top(3):
      logging.info(f"{word.decode()}: {c}")
  except:
    for file in glob.glob(f"{OUTPUT_DIR}/*"):
      os.remove(file)
    os.rmdir("output")

