import time
import MySQLdb as mdb
from thread_base import ThreadBase
from preprocessors import (MysqlPreprocessor, InnoDBPreprocessor)


class ThreadMySQLMaxReconnectException(Exception):
    pass


class ThreadMySQL(ThreadBase):
    """ Polls mysql and inserts data into queue """
    is_running = True
    connection = None
    reconnect_attempt = 0
    max_reconnect = 30
    die_on_max_reconnect = False
    stats_checks = {}
    check_lastrun = {}

    def __init__(self, *args, **kwargs):
        super(ThreadMySQL, self).__init__(*args, **kwargs)
        self.processor_class_mysql = MysqlPreprocessor()
        self.processor_class_inno = InnoDBPreprocessor()

    def configure(self, config_dict):
        self.host = config_dict.get('mysql').get('host', 'localhost')
        self.port = config_dict.get('mysql').get('port', 3306)

        self.username = config_dict.get('mysql').get('username', 'root')
        self.password = config_dict.get('mysql').get('password', '')

        #Set the stats checks for MySQL
        for stats_type in config_dict.get('mysql').get('stats_types').split(','):
            if config_dict.get('mysql').get('query_'+stats_type) and \
                    config_dict.get('mysql').get('interval_'+stats_type):

                self.stats_checks[stats_type] = {
                    'query': config_dict.get('mysql').get('query_'+stats_type),
                    'interval': config_dict.get('mysql').get('interval_'+stats_type)
                }
                self.check_lastrun[stats_type] = (time.time()*1000)

        self.sleep_interval = int(config_dict.get('mysql').get('sleep_interval', 500))/1000

        #Which metrics do we allow to be sent to the backend?
        self.metrics = config_dict.get('metrics')

        return self.host, self.port, self.sleep_interval

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

                """
                Pre process rows
                This transforms innodb status to a row like structure
                This allows pluggable modules,
                preprocessors should return list of key value tuples, e.g.:
                [('my_key', '1'), (my_counter, '2'), ('another_metric', '666')]
                """
                rows = self._preprocess(check_type, cursor.fetchall())
                for key, value in rows:
                    metric_key = check_type+"."+key.lower()
                    metric_type = self.metrics.get(metric_key)

                    # Only allow the whitelisted metrics to be sent off to Statsd
                    if metric_key in self.metrics:
                        self.queue.put((metric_key, value, metric_type))
                self.check_lastrun[check_type] = time_now

        """ Sleep if necessary """
        time.sleep(self.sleep_interval)

    def _preprocess(self, check_type, rows):
        """
        Return rows when type not innodb.
        This is done to make it transparent for furture transformation types
        """
        executing_class = self.processor_class_mysql
        if check_type == 'innodb':
            executing_class = self.processor_class_inno

        return executing_class.process(rows)

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
