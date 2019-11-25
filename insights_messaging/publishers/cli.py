from . import Publisher


class StdOut(Publisher):
    def publish(self, input_msg, response):
        print(response)
