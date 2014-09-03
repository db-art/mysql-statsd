import threading


class ThreadBase(threading.Thread):
    run = True

    def __init__(self, queue, **kwargs):
        threading.Thread.__init__(self)
        self.queue = queue
        self.data = {}
        if getattr(self, 'configure', None):
            self.configure(kwargs)

    def stop(self):
        self.run = False
