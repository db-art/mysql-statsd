#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import Queue
import signal
import sys, os
import threading
import time
from ConfigParser import ConfigParser

from thread_manager import ThreadManager
from thread_mysql import ThreadMySQL
#from thread_mysql_status import ThreadMySQLStatus
from thread_statsd import ThreadStatsd

op = argparse.ArgumentParser()
op.add_argument("-c", "--config", dest="file", default="/etc/mysql-statsd.conf", help="Configuration file")
op.add_argument("-d", "--debug", dest="debug", help="Debug mode", default=False, action="store_true")
opt = op.parse_args()


class MysqlStatsd():
    """Main program class"""
    opt = None
    config = None

    def __init__(self):
        """Program entry point"""
        global cnf
        self.config = cnf

        # Set up queue
        self.queue = Queue.Queue()

        # Spawn MySQL polling thread

        t1 = ThreadMySQL(queue=self.queue, **self.config)
        # t1 = ThreadMySQL(config=self.config, queue=self.queue)

        # Spawn Statsd flushing thread
        t2 = ThreadStatsd(queue=self.queue, **self.config['statsd'])

        # Get thread manager
        tm = ThreadManager(threads=[t1, t2])
        tm.run()

def get_config(config_file):
    cnf = ConfigParser()
    cnf.read(config_file)[0]
    config = {}
    for section in cnf.sections():
        config[section] = {}
        for key, value in cnf.items(section):
            config[section][key] = value

    return config


def daemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    global cnf

    '''This forks the current process into a daemon. The stdin, stdout, and
    stderr arguments are file names that will be opened and be used to replace
    the standard file descriptors in sys.stdin, sys.stdout, and sys.stderr.
    These arguments are optional and default to /dev/null. Note that stderr is
    opened unbuffered, so if it shares a file with stdout then interleaved
    output may not appear in the order that you expect. '''

    # Do first fork.
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)   # Exit first parent.
    except OSError, e:
        sys.stderr.write ("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    # Decouple from parent environment.
    os.chdir("/")
    os.umask(0)
    os.setsid()

    # Do second fork.
    try:
        pid = os.fork()
        if pid > 0:
            f = open(cnf.get('daemon').get('pidfile', '/var/run/mysql_statsd.pid'), 'w')
            f.write(str(pid))
            f.close()
            sys.exit(0)   # Exit second parent.
    except OSError, e:
        sys.stderr.write ("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror) )
        sys.exit(1)

    # Now I am a daemon!

    # Redirect standard file descriptors.
    si = open(stdin, 'r')
    so = open(stdout, 'a+')
    se = open(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

cnf = get_config(opt.file)
if __name__ == "__main__":
    daemonize('/dev/null',cnf.get('daemon').get('logfile','/bigdisk/logs/mysql_statsd/daemon.log'),cnf.get('daemon').get('logfile','/bigdisk/logs/mysql_statsd/daemon.log'))
    program = MysqlStatsd()

