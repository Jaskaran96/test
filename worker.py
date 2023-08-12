import logging
from typing import Any
import multiprocessing
from base import Worker
from constants import FNAME,IN
from mrds import MyRedis
from redis.client import Redis
import os
import json
import pandas as pd
stream_name = IN
group_name = Worker.GROUP
import time



class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    self.rds: MyRedis = kwargs['rds']
    self.localdic = {}
    self.process_name = multiprocessing.current_process().name
    self.pid = os.getpid()
    while True:
      try:
          resp = self.rds.rds.xreadgroup(group_name,self.process_name,{stream_name : '>'},count=1, block=50)
          if resp:
              key, messages = resp.pop()
              # print(messages[0][1][b'tweet'])
              last_id= messages[0][0]
              data = messages[0][1][b'tweet'].decode()
              print("      --> ", data)
              self.rds.rds.xack(stream_name, group_name, last_id)
              self.loadCSV(data)
          else:
              break
      except ConnectionError as e:
            print("ERROR REDIS CONNECTION: {}".format(e))
    with open(f"output/{self.process_name}", 'w') as convert_file:
      convert_file.write(json.dumps(self.localdic))
    # self.ingestData()
    logging.info("Exiting")
  
  def loadCSV(self,fileName):
    df = pd.read_csv(fileName,usecols=['text'])
    for _, row in df.iterrows():
      for word in row['text'].split():
          try:
            self.localdic[word]+=1
          except:
            self.localdic[word]=1
    print("************************************************************************************************************************")
  



