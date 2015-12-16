import time
import re
import MySQLdb as mdb
import traceback
from thread_base import ThreadBase
from preprocessors import (MysqlPreprocessor, InnoDBPreprocessor, ColumnsPreprocessor)


class ThreadMySQLMaxReconnectException(Exception):
    pass


class ThreadMySQL(ThreadBase):
    """ Polls mysql and inserts data into queue """
    is_running = True
    connection = None
    recovery_attempt = 0
    reconnect_delay = 5
    stats_checks = {}
    check_lastrun = {}

    def __init__(self, *args, **kwargs):
        super(ThreadMySQL, self).__init__(*args, **kwargs)
        self.processor_class_mysql = MysqlPreprocessor()
        self.processor_class_inno = InnoDBPreprocessor()
        self.processor_class_columns = ColumnsPreprocessor()

    def configure(self, config_dict):
        self.host = config_dict.get('mysql').get('host', 'localhost')
        self.port = config_dict.get('mysql').get('port', 3306)
        self.socket = config_dict.get('mysql').get('socket', None)

        self.username = config_dict.get('mysql').get('username', 'root')
        self.password = config_dict.get('mysql').get('password', '')

        self.max_reconnect = int(config_dict.get('mysql').get('max_reconnect', 5))
        self.max_recovery = int(config_dict.get('mysql').get('max_recovery', 10))
        
        #Set the stats checks for MySQL
        for stats_type in config_dict.get('mysql').get('stats_types').split(','):
            if config_dict.get('mysql').get('query_'+stats_type) and \
                    config_dict.get('mysql').get('interval_'+stats_type):

                self.stats_checks[stats_type] = {
                    'query': config_dict.get('mysql').get('query_'+stats_type),
                    'interval': config_dict.get('mysql').get('interval_'+stats_type)
                }
                self.check_lastrun[stats_type] = (time.time()*1000)

        self.sleep_interval = int(config_dict.get('mysql').get('sleep_interval', 500))/1000.0

        #Which metrics do we allow to be sent to the backend?
        self.metrics = config_dict.get('metrics')

        return self.host, self.port, self.sleep_interval

    def setup_connection(self):
        connection_attempt = 0

        while self.max_reconnect == 0 or connection_attempt < self.max_reconnect:
            try:
                if self.socket:
                    self.connection = mdb.connect(user=self.username, unix_socket=self.socket, passwd=self.password)
                else:
                    self.connection = mdb.connect(host=self.host, user=self.username, port=self.port, passwd=self.password)

                return self.connection
            except Exception:
                pass

            # If we got here, connection failed
            connection_attempt += 1
            print('Attempting reconnect #{0}...'.format(connection_attempt))
            time.sleep(self.reconnect_delay)
        
        # If we get out of the while loop, we've passed max_reconnect
        raise ThreadMySQLMaxReconnectException


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
        for check_type in self.stats_checks:
            """
            Only run a check if we exceeded the query threshold.
            This is especially important for SHOW INNODB ENGINE
            which locks the engine for a short period of time
            """
            time_now = time.time()*1000
            check_threshold = float(self.stats_checks[check_type]['interval'])
            check_lastrun = self.check_lastrun[check_type]
            if (time_now - check_lastrun) > check_threshold:
                cursor = self.connection.cursor()
                cursor.execute(self.stats_checks[check_type]['query'])
                column_names = [i[0] for i in cursor.description]

                """
                Pre process rows
                This transforms innodb status to a row like structure
                This allows pluggable modules,
                preprocessors should return list of key value tuples, e.g.:
                [('my_key', '1'), (my_counter, '2'), ('another_metric', '666')]
                """
                rows = self._preprocess(check_type, column_names, cursor.fetchall())
                for key, value in rows:
                    key = key.lower()
                    metric_key = check_type + "." + key

                    # Support multiple bufferpools in metrics (or rather: ignore them)
                    # Bascially bufferpool_* whitelists metrics for *all* bufferpools
                    if key.startswith('bufferpool_'):
                        # Rewrite the metric key to the whitelisted wildcard key
                        whitelist_key = "{0}.{1}".format(check_type, re.sub(r'(.*bufferpool_)\d+(\..+)', r'\1*\2', key))

                        # Only allow the whitelisted metrics to be sent off to Statsd
                        if whitelist_key in self.metrics:
                            metric_type = self.metrics.get(whitelist_key)
                            self.queue.put((metric_key, value, metric_type))
                    else:
                        # Only allow the whitelisted metrics to be sent off to Statsd
                        if metric_key in self.metrics:
                            metric_type = self.metrics.get(metric_key)
                            self.queue.put((metric_key, value, metric_type))
                self.check_lastrun[check_type] = time_now

    def _preprocess(self, check_type, column_names, rows):
        """
        Return rows when type not innodb.
        This is done to make it transparent for furture transformation types
        """
        extra_args = ()

        executing_class = self.processor_class_mysql
        if check_type == 'innodb':
            executing_class = self.processor_class_inno
        if check_type == 'slave':
            executing_class = self.processor_class_columns
            extra_args = (column_names,)

        return executing_class.process(rows, *extra_args)

    def recover_errors(self, ex):
        """Decide whether we should continue."""
        if self.max_recovery > 0 and self.recovery_attempt >= self.max_recovery:
            print("Giving up after {} consecutive errors".format(self.recovery_attempt))
            raise

        self.recovery_attempt += 1
        print("Ignoring database error:")
        traceback.print_exc()

        # Server gone away requires we reset the connection.
        if ex.args[0] == 2006:
            self.connection.close()

    def run(self):
        """ Run forever """
        if not self.connection:
            """ Initial connection setup """
            self.setup_connection()

        while self.is_running:
            if not self.connection.open:
                self.setup_connection()

            try:
                self._run()
                self.recovery_attempt = 0
            except mdb.DatabaseError as ex:
                self.recover_errors(ex)

            time.sleep(self.sleep_interval)
