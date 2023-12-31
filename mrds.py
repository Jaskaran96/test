from __future__ import annotations
from typing import Final
from redis.client import Redis
from base import Worker
from constants import IN, COUNT, FNAME


class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379,
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.rds.xgroup_create(IN, Worker.GROUP, id="0", mkstream=True)

  def add_file(self, fname: str):
    self.rds.xadd(IN, {FNAME: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(COUNT, '+inf', '-inf', 0, n,
                                     withscores=True)
                       