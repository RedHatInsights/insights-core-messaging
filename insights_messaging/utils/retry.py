import logging
from typing import Callable, Any
from pika.exceptions import AMPQConnectionError, ChannelClosed
from time import sleep

log = logging.getLogger(__name__)

class RetryDecorator:
    def __init__(self, func: Callable[[Any], None]):
        self._func = func
        self._sleep_time = 1

    def __call__(self, *args, **kwargs):
        while True:
            try:
                return self._func(*args, **kwargs)
            except (AMPQConnectionError, ChannelClosed) as e:
                log.error(f'Caught exception {e}. Trying again in {self._sleep_time} seconds')
                sleep(self._sleep_time)
                if self._sleep_time < 3:
                    self._sleep_time += 1


def retry(func):
    return RetryDecorator(func)
