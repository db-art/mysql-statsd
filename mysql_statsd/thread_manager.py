import Queue
import signal
import threading
import time


class ThreadManager():
    """Knows how to manage dem threads"""
    quit = False
    quitting = False
    threads = []

    def __init__(self, threads=[]):
        """Program entry point"""
        self.threads = threads
        self.register_signal_handlers()

    def register_signal_handlers(self):
        # Register signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def run(self):
        """Main loop."""
        self.start_threads()
        while not self.quit:
            time.sleep(1)

            dead = [thread for thread in self.threads if not thread.is_alive()]
            if dead and not self.quitting:
                print("Thread {0!r} has stopped unexpectedly.".format(thread))
                self.stop_threads()
                return

    def start_threads(self):
        for t in self.threads:
            t.start()

    def signal_handler(self, signal, frame):
        """ Handle signals """
        print("Caught CTRL+C / SIGKILL")
        if not self.quitting:
            self.quitting = True
            self.stop_threads()
            self.quit = True
        else:
            print("BE PATIENT!@#~!#!@#$~!`1111")

    def stop_threads(self):
        """Stops all threads and waits for them to quit"""
        print("Stopping threads")
        for thread in self.threads:
            thread.stop()
        while threading.activeCount() > 1:
            print("Waiting for %s threads" % threading.activeCount())
            time.sleep(1)
        print("All threads stopped")
