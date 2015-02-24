import Queue
import random
import string
import time
import socket
import distutils.util
from pystatsd import statsd
from thread_base import ThreadBase


class ThreadGenerateGarbage(ThreadBase):
    """
    stat, value, type
    c = counter, t = timer, g = gauge
    (stat, x, type)
    """
    def gen_key(self):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for x in range(8))

    def run(self):
        while self.run:
            time.sleep(1)
            self.queue.put((self.gen_key(), random.randint(0, 1000), 'c'))


class ThreadStatsd(ThreadBase):
    debug = False

    def configure(self, config):
        host = config.get('host', 'localhost')
        port = int(config.get('port', 8125))
        prefix = config.get('prefix', 'mysql_statsd')
        if distutils.util.strtobool(config.get('include_hostname', 'mysql_statsd')):
            prefix += "." + socket.gethostname().replace('.', '_')
        self.client = statsd.Client(host, port, prefix=prefix)

    def get_sender(self, t):
        if t == 'g':
            return self.client.gauge
        elif t in ['r', 'd']:
            return self.client.update_stats
        elif t == 'c':
            return self.client.incr
        elif t == 't':
            return self.client.timing

    def send_stat(self, item):
        (k, v, t) = item

        # Don't proceed if we don't have data
        if v == None:
            return False

        if t == 'd':
            delta = self.get_delta(k, v)
            if delta > 0:
                sender = self.get_sender(t)
                sender(k, float(delta))
        else:
            sender = self.get_sender(t)
            sender(k, float(v))

    def get_delta(self, k, v):
        if k in self.data:
            delta = float(v) - self.data[k]
            self.data[k] = float(v)
            return delta
        else:
            self.data[k] = float(v)
            return -1

    def run(self):
        while self.run:
            try:
                # Timeout after 1 second so we can respond to quit events
                item = self.queue.get(True, 1)
                if self.debug:
                    print(item)
                self.send_stat(item)
            except Queue.Empty:
                continue


class ThreadFakeStatsd(ThreadStatsd):
    """Prints metrics instead of sending them to statsd."""
    def send_stat(self, item):
        print item


if __name__ == '__main__':
    # Run standalone to test this module, it will generate garbage
    from thread_manager import ThreadManager
    q = Queue.Queue()

    threads = [ThreadGenerateGarbage(q), ThreadStatsd(q)]
    tm = ThreadManager(threads=threads)
    tm.run()
