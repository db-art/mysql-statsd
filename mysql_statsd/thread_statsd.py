import threading
import time


class ThreadStatsd(threading.Thread):
    run = True

    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self)

    def stop(self):
        self.run = False

    def run(self):
        while self.run:
            time.sleep(5)
            print('Statsd: bluup!')
