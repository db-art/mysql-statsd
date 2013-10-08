import threading
import time
import Queue
import MySQLdb as mdb
import datetime
from thread_base import ThreadBase

class ThreadMySQLMaxReconnectException(Exception):
    pass

class ThreadMySQL(ThreadBase):
    """ Polls mysql and inserts data into queue """
    is_running = True
    connection = None
    reconnect_attempt = 0
    max_reconnect = 30
    die_on_max_reconnect = False
    data_query = "SHOW GLOBAL STATUS"


    def configure(self, config_dict):
        self.host = config_dict.get('host', 'localhost')
        self.port = config_dict.get('port', 3306)

        self.username = config_dict.get('username', 'root')
        self.password = config_dict.get('password', '')
        self.query_interval = int(config_dict.get('query_interval', 1000)) / 1000

        return self.host, self.port, self.query_interval

    def setup_connection(self):
        self.connection = mdb.connect(host=self.host, user=self.username, passwd=self.password)
        return self.connection

    def stop(self):
        """ Stop running this thread and close connection """
        self.is_running = False
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            """ Ignore exceptions thrown during closing connection """
            pass

    def _run(self):
        if not self.connection.open:
            self.reconnect()

        cursor = self.connection.cursor()
        cursor.execute(self.data_query)
        rows = cursor.fetchall()
        
        for key, value in rows: 
            self.queue.put((key.lower(), value, 'c'))

        time.sleep(self.query_interval)

    def reconnect(self):
        if self.die_on_max_reconnect and self.reconnect_attempt >= self.max_reconnect:
            raise ThreadMySQLMaxReconnectException

        self.reconnect_attempt += 1
        print('Attempting reconnect #{0}...'.format(self.reconnect_attempt))
        self.setup_connection()
        

    def run(self):
        """ Run forever """
        
        if not self.connection:
            """ Initial connection setup """
            self.setup_connection()

        while self.is_running:
            self._run()

