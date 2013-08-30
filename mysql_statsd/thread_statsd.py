import Queue
import random
import string
import threading
import time
from pystatsd import statsd


# stat, value, type
# c = counter, t = timer, g = gauge
# (stat, x, type)
class ThreadGenerateGarbage(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def gen_key(self):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for x in range(8))

    def stop(self):
        self.run = False

    def run(self):
        while self.run:
            time.sleep(1)
            self.queue.put((self.gen_key(), random.randint(0,1000), 'c'))


class ThreadStatsd(threading.Thread):
    run = True

    def __init__(self, queue, **kwargs):
        threading.Thread.__init__(self)
        self.queue = queue
        self.configure(kwargs)

    def configure(self, config):
        host = config.get('host', 'localhost')
        port = int(config.get('port', 8125))
        prefix = config.get('prefix', 'mysql_statsd')
        self.client = statsd.Client(host, port, prefix=prefix)

    def get_sender(self, t):
        if t is 'g':
            return self.client.gauge
        elif t is 'c':
            return self.client.incr
        elif t is 't':
            return self.client.timing

    def send_stat(self, item):
        (k, v, t) = item
        sender = self.get_sender(t)
        sender(k, v)

    def stop(self):
        self.run = False

    def run(self):
        while self.run:
            try:
                # Timeout after 1 second so we can respond to quit events
                item = self.queue.get(True, 1)
                self.send_stat(item)
            except Queue.Empty:
                continue


if __name__ == '__main__':
    # Run standalone to test this module, it will generate garbage
    q = Queue.Queue()
    t1 = ThreadGenerateGarbage(q)
    t2 = ThreadStatsd(q)
    t1.start()
    t2.start()
    while True:
        try:
            time.sleep(1)
        except:
            t1.stop()
            t2.stop()
