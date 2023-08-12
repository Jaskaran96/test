from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final
from multiprocessing import Process


class Worker(ABC):
  GROUP: Final = "worker"

  def __init__(self, **kwargs: Any):
    self.name = "worker-?"
    self.pid = -1

  def create_and_run(self, **kwargs: Any) -> None:
    p = Process(target=self.run,kwargs=kwargs)
    p.start()
    self.pid = p.pid
    self.name = p.name
    print(f"Started process {self.name} with ID {self.pid}")

  @abstractmethod
  def run(self, **kwargs: Any) -> None:
    pass

  def kill(self) -> None:
    logging.info(f"Killing {self.name}")
    os.kill(self.pid, signal.SIGKILL)
