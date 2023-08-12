import logging
from typing import Any
import multiprocessing
from base import Worker
from constants import FNAME
from mrds import MyRedis
from redis.client import Redis
import os
stream_name = 'fname'
group_name = Worker.GROUP

class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    # rds: MyRedis = kwargs['rds']
    # self.dic = {}
    # self.rds = Redis(host='localhost', port=6379, password='pass',
    #                    db=0, decode_responses=False)
    # self.process_name = multiprocessing.current_process().name
    # self.pid = os.getpid()
    # while True:
    #   try:
    #       resp = self.rds.xreadgroup(group_name,self.process_name,{stream_name : '>'},count=1, block=50)
    #       if resp:
    #           key, messages = resp.pop()
    #           last_id, data = messages[0]
    #           #print(consumer_name,last_id)
    #           # print("REDIS ID: ", last_id)
    #           print("      --> ", data)
    #           count+=1
    #           self.rds.xack(stream_name, group_name, last_id)
    #       else:
    #           break
    #   except ConnectionError as e:
    #         print("ERROR REDIS CONNECTION: {}".format(e))

    logging.info("Exiting")
  
  def loadCSV(self):
    pass

