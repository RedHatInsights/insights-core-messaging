import logging
from . import Consumer

log = logging.getLogger(__name__)


class Interactive(Consumer):
    def run(self):
        while True:
            msg = input("Input Archive Name: ")
            if not msg:
                break
            self.process(msg)

    def get_url(self, input_msg):
        return input_msg
