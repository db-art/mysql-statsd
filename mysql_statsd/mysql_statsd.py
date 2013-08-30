#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import Queue
import signal
import sys
import threading
import time

from thread_mysql import ThreadMySQL
from thread_statsd import ThreadStatsd


class MysqlStatsd():
    """Main program class"""
    stop_threads = False
    threads = []
    queue = None
    quitting = False
    quit = False
    opt = None
    config = None

    def __init__(self):
        """Program entry point"""
        op = argparse.ArgumentParser()

        op.add_argument("-c", "--config", dest="file", default="/etc/mysql-statsd.conf", help="Configuration file")
        op.add_argument("-d", "--debug", dest="debug", help="Debug mode", default=False, action="store_true")

        self.opt = op.parse_args()
        opt = self.opt

        # Set up queue
        self.queue = Queue.Queue()

        # Register signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Spawn MySQL polling thread
        t = ThreadMySQL(config=self.config, queue=self.queue)
        t.start()
        self.threads.append(t)

        # Spawn Statsd flushing thread
        t = ThreadStatsd(config=self.config, queue=self.queue)
        t.start()
        self.threads.append(t)

        while not self.quit:
            # Do a whole lot of nothing
            time.sleep(10)
            print('Bleep')

        if self.quit:
            # We got here by a quit signal, not by queue depletion
            sys.exit(0)


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


if __name__ == '__main__':
    program = MysqlStatsd()
