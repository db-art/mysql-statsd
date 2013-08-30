#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import Queue
import signal
import sys
import threading
import time

from thread_manager import ThreadManager
from thread_mysql import ThreadMySQL
from thread_statsd import ThreadStatsd


class MysqlStatsd():
    """Main program class"""
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

        # Spawn MySQL polling thread
        t1 = ThreadMySQL(config=self.config, queue=self.queue)

        # Spawn Statsd flushing thread
        t2 = ThreadStatsd(config=self.config, queue=self.queue)

        # Get thread manager
        tm = ThreadManager(threads=[t1, t2])

        tm.run()


if __name__ == '__main__':
    program = MysqlStatsd()
