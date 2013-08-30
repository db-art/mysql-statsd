#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import Queue
import signal
import sys
import threading
import time
from ConfigParser import ConfigParser

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
        op.add_argument("-q", "--query-interval", dest="query_interval", default=1000, help="Interval to poll mysql for new data in miliseconds")

        self.opt = op.parse_args()
        opt = self.opt

        self.get_config(opt.file)

        # Set up queue
        self.queue = Queue.Queue()

        # Spawn MySQL polling thread

        t1 = ThreadMySQL(queue=self.queue, **self.config['mysql'])
        # t1 = ThreadMySQL(config=self.config, queue=self.queue)

        # Spawn Statsd flushing thread
        t2 = ThreadStatsd(queue=self.queue, **self.config['statsd'])

        # Get thread manager
        tm = ThreadManager(threads=[t1, t2])
        tm.run()

    def get_config(self, config_file):
        cnf = ConfigParser()
        cnf.read(config_file)[0]
        self.config = {}
        for section in cnf.sections():
            self.config[section] = {}
            for key, value in cnf.items(section):
                self.config[section][key] = value

        return self.config

if __name__ == '__main__':
    program = MysqlStatsd()
