#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import Queue
import signal
import sys
import os
import threading
import time
from ConfigParser import ConfigParser

from thread_manager import ThreadManager
from thread_mysql import ThreadMySQL
from thread_statsd import ThreadStatsd, ThreadFakeStatsd


class MysqlStatsd():
    """Main program class"""
    opt = None
    config = None

    def __init__(self):
        """Program entry point"""
        op = argparse.ArgumentParser()
        op.add_argument("-c", "--config", dest="cfile",
                default="/etc/mysql-statsd.conf",
                help="Configuration file"
        )
        op.add_argument("-d", "--debug", dest="debug",
                help="Prints statsd metrics next to sending them",
                default=False, action="store_true"
        )
        op.add_argument("--dry-run", dest="dry_run",
                default=False,
                action="store_true",
                help="Print the output that would be sent to statsd without actually sending data somewhere"
        )

        # TODO switch the default to True, and make it fork by default in init script.
        op.add_argument("-f", "--foreground", dest="foreground", help="Dont fork main program", default=False, action="store_true")

        opt = op.parse_args()
        self.get_config(opt.cfile)

        if not self.config:
            sys.exit(op.print_help())

        try:
            logfile = self.config.get('daemon').get('logfile', '/tmp/daemon.log')
        except AttributeError:
            logfile = sys.stdout
            pass

        if not opt.foreground:
            self.daemonize(stdin='/dev/null', stdout=logfile, stderr=logfile)

        # Set up queue
        self.queue = Queue.Queue()

        # split off config for each thread
        mysql_config = dict(mysql=self.config['mysql'])
        mysql_config['metrics'] = self.config['metrics']

        statsd_config = self.config['statsd']

        # Spawn MySQL polling thread
        mysql_thread = ThreadMySQL(queue=self.queue, **mysql_config)
        # t1 = ThreadMySQL(config=self.config, queue=self.queue)

        # Spawn Statsd flushing thread
        statsd_thread = ThreadStatsd(queue=self.queue, **statsd_config)

        if opt.dry_run:
            statsd_thread = ThreadFakeStatsd(queue=self.queue, **statsd_config)

        if opt.debug:
            """ All debug settings go here """
            statsd_thread.debug = True

        # Get thread manager
        tm = ThreadManager(threads=[mysql_thread, statsd_thread])

        try:
            tm.run()
        except:
            # Protects somewhat from needing to kill -9 if there is an exception
            # within the thread manager by asking for a quit an joining.
            try:
                tm.stop_threads()
            except:
                pass

            raise

    def get_config(self, config_file):
        cnf = ConfigParser()
        try:
            cnf.read(config_file)[0]
        except IndexError:
            # Return None so we can display help...
            self.config = None  # Just to be safe..
            return None

        self.config = {}
        for section in cnf.sections():
            self.config[section] = {}
            for key, value in cnf.items(section):
                self.config[section][key] = value

        return self.config

    def daemonize(self, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
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
            sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)

        # Decouple from parent environment.
        # TODO: do we need to change to '/' or can we chdir to wherever __file__ is?
        os.chdir("/")
        os.umask(0)
        os.setsid()

        # Do second fork.
        try:
            pid = os.fork()
            if pid > 0:
                f = open(self.config.get('daemon').get('pidfile', '/var/run/mysql_statsd.pid'), 'w')
                f.write(str(pid))
                f.close()
                sys.exit(0)   # Exit second parent.
        except OSError, e:
            sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
            sys.exit(1)

        # Now I am a daemon!

        # Redirect standard file descriptors.
        si = open(stdin, 'r')
        so = open(stdout, 'a+')
        se = open(stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

if __name__ == "__main__":
    program = MysqlStatsd()
