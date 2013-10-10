import threading
import time
import Queue
import MySQLdb as mdb
import datetime
import re
from thread_base import ThreadBase

class ThreadMySQLMaxReconnectException(Exception):
    pass

class ThreadMySQL(ThreadBase):
    """ Polls mysql and inserts data into queue """
    run = True
    connection = None
    reconnect_attempt = 0
    max_reconnect = 30
    die_on_max_reconnect = False
    stats_checks = {}
    check_lastrun = {}

    def configure(self, config_dict):
        self.host = config_dict.get('mysql').get('host', 'localhost')
        self.port = config_dict.get('mysql').get('port', 3306)

        self.username = config_dict.get('mysql').get('username', 'root')
        self.password = config_dict.get('mysql').get('password', '')

        #Set the stats checks for MySQL
        for type in config_dict.get('mysql').get('stats_types').split(','):
            if config_dict.get('mysql').get('query_'+type) and config_dict.get('mysql').get('interval_'+type):
                self.stats_checks[type] = {'query': config_dict.get('mysql').get('query_'+type), 'interval': config_dict.get('mysql').get('interval_'+type)}
                self.check_lastrun[type] = (time.time()*1000)

        self.sleep_interval = int(config_dict.get('mysql').get('sleep_interval', 500))/1000

        #Which metrics do we allow to be sent to the backend?
        self.metrics = config_dict.get('metrics')

        return self.host, self.port, self.sleep_interval

    def setup_connection(self):
        self.connection = mdb.connect(host=self.host, user=self.username, passwd=self.password)
        return self.connection

    def stop(self):
        """ Stop running this thread and close connection """
        self.run = False
        try:
            if self.connection:
                self.connection.close()
        except Exception:
            """ Ignore exceptions thrown during closing connection """
            pass

    def _run(self):
        if not self.connection.open:
                self.reconnect_attempt += 1
                print('Attempting reconnect #{0}...'.format(self.reconnect_attempt))
                self.setup_connection()

        for check_type in self.stats_checks:
            #Only run a check if we exceeded the query threshold. This is especially important for SHOW INNODB ENGINE which locks the engine for a short period of time
            if ((time.time()*1000) - self.check_lastrun[check_type]) > float(self.stats_checks[check_type]['interval']):
                cursor = self.connection.cursor()
                cursor.execute(self.stats_checks[check_type]['query'])
                #Pre process rows. This transforms innodb status to a row like structure
                rows = self._preprocess(check_type, cursor.fetchall())
                for key, value in rows:
                    if check_type+"."+key.lower() in self.metrics:
                        self.queue.put((check_type+"."+key.lower(), value, self.metrics.get(check_type+"."+key.lower())))
                self.check_lastrun[check_type] = (time.time()*1000)
        
        time.sleep(self.sleep_interval)
        

    def _preprocess(self, check_type, rows):
        #Return rows when type not innodb. This is done to make it transparent for furture transformation types
        if check_type != 'innodb':
           return rows

        p = re.compile(r'\s+')
        tmp_stats = {}
        txn_seen = 0
        prev_line = ''
        for row in rows:
            innoblob = row[2].replace(',', '').replace(';', '').replace('/s', '').split('\n');
            #innoblob = row[2].split('\n');
            for line in innoblob:
                innorow = p.split(line);
                if line.startswith('Mutex spin waits'):
                   # Mutex spin waits 79626940, rounds 157459864, OS waits 698719
                   # Mutex spin waits 0, rounds 247280272495, OS waits 316513438
                    tmp_stats['spin_waits'] = innorow[3]
                    tmp_stats['spin_rounds'] = innorow[5]
                    tmp_stats['os_waits'] = innorow[8]
                elif line.startswith('RW-shared spins') and ';' in line:
                    # RW-shared spins 3859028, OS waits 2100750; RW-excl spins 4641946, OS waits 1530310
                    tmp_stats['spin_waits'] = innorow[2]
                    tmp_stats['spin_waits'] = innorow[8]
                    tmp_stats['os_waits']  = innorow[5]
                    tmp_stats['os_waits']  += innorowrow[11]
                elif line.startswith('RW-shared spins') and '; RW-excl spins' in line:
                    # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
                    # RW-shared spins 604733, rounds 8107431, OS waits 241268
                    tmp_stats['spin_waits'] = innorow[2]
                    tmp_stats['os_waits'] = innorow[7]
                elif line.startswith('RW-excl spins'):
                    # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
                    # RW-excl spins 604733, rounds 8107431, OS waits 241268
                    tmp_stats['spin_waits'] = innorow[2]
                    tmp_stats['os_waits'] = innorow[7]
                elif 'seconds the semaphore:' in line:
                    # --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for 1.00 seconds the semaphore:
#                    if tmp_stats['innodb_sem_waits'] == None:
                    tmp_stats = self.increment(tmp_stats, 'innodb_sem_waits', 1)
#                    else:
#                        tmp_stats['innodb_sem_waits'] += 1
                    if 'innodb_sem_wait_time_ms' in tmp_stats:
                        tmp_stats['innodb_sem_wait_time_ms'] = float(tmp_stats['innodb_sem_wait_time_ms']) + float(innorow[9]) * 1000;
                    else:
                        tmp_stats['innodb_sem_wait_time_ms'] = float(innorow[9]) * 1000;
                # TRANSACTIONS
                elif line.startswith('Trx id counter'):
                    # The beginning of the TRANSACTIONS section: start counting
                    # transactions
                    # Trx id counter 0 1170664159
                    # Trx id counter 861B144C
                    if len(innorow) == 4:
                        innorow.append(0)
                    tmp_stats['innodb_transactions'] = self.make_bigint(innorow[3], innorow[4])
                    txn_seen = 1
                elif line.startswith('Purge done for trx'):
                    # Purge done for trx's n:o < 0 1170663853 undo n:o < 0 0
                    # Purge done for trx's n:o < 861B135D undo n:o < 0
                    if innorow[7] == 'undo':
                        innorow[7] = 0
                    tmp_stats['unpurged_txns'] = tmp_stats['innodb_transactions'] - self.make_bigint(innorow[6], innorow[7])
                elif line.startswith('History list length'):
                    # History list length 132
                    tmp_stats['history_list'] = innorow[3]
                elif txn_seen == 1 and line.startswith('---TRANSACTION'):
                    # ---TRANSACTION 0, not started, process no 13510, OS thread id 1170446656
                    tmp_stats = self.increment(tmp_stats, 'current_transactions', 1)
                    if 'ACTIVE' in line:
                        tmp_stats = self.increment(tmp_stats, 'active_transactions', 1);
                elif txn_seen == 1 and line.startswith('------- TRX HAS BEEN'):
                    # ------- TRX HAS BEEN WAITING 32 SEC FOR THIS LOCK TO BE GRANTED:
                    tmp_stats = self.increment(tmp_stats, 'innodb_lock_wait_secs', innorow[5])
                elif 'read views open inside InnoDB' in line:
                    # 1 read views open inside InnoDB
                    tmp_stats['read_views'] = innorow[0]
                elif line.startswith('mysql tables in use'):
                    # mysql tables in use 2, locked 2
                    tmp_stats = self.increment(tmp_stats, 'innodb_tables_in_use', innorow[4])
                    tmp_stats = self.increment(tmp_stats, 'innodb_locked_tables', innorow[6])
                elif txn_seen == 1 and 'lock struct(s)' in line:
                    # 23 lock struct(s), heap size 3024, undo log entries 27
                    # LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
                    # LOCK WAIT 2 lock struct(s), heap size 368
                    if line.startswith( 'LOCK WAIT'):
                        tmp_stats = self.increment(tmp_stats, 'innodb_lock_structs', innorow[2])
                        tmp_stats = self.increment(tmp_stats, 'locked_transactions', 1)
                    else:
                        tmp_stats = self.increment(tmp_stats, 'innodb_lock_structs', innorow[0])
                # FILE I/O
                elif ' OS file reads, ' in line:
                    # 8782182 OS file reads, 15635445 OS file writes, 947800 OS fsyncs
                    tmp_stats['file_reads']  = innorow[0];
                    tmp_stats['file_writes'] = innorow[4];
                    tmp_stats['file_fsyncs'] = innorow[8];
                elif line.startswith( 'Pending normal aio reads:'):
                    # Pending normal aio reads: 0, aio writes: 0,
                    tmp_stats['pending_normal_aio_reads']  = innorow[4];
                    tmp_stats['pending_normal_aio_writes'] = innorow[7];
                elif line.startswith('ibuf aio reads'):
                    #  ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
                    tmp_stats['pending_ibuf_aio_reads'] = innorow[3]
                    tmp_stats['pending_aio_log_ios']    = innorow[6]
                    tmp_stats['pending_aio_sync_ios']   = innorow[9]
                elif line.startswith('Pending flushes (fsync)'):
                    # Pending flushes (fsync) log: 0; buffer pool: 0
                    tmp_stats['pending_log_flushes']      = innorow[4]
                    tmp_stats['pending_buf_pool_flushes'] = innorow[7]
                elif line.startswith( 'Ibuf for space 0: size '):
                    # Older InnoDB code seemed to be ready for an ibuf per tablespace.  It
                    # had two lines in the output.  Newer has just one line, see below.
                    # Ibuf for space 0: size 1, free list len 887, seg size 889, is not empty
                    # Ibuf for space 0: size 1, free list len 887, seg size 889,
                    tmp_stats['ibuf_used_cells']  = innorow[5]
                    tmp_stats['ibuf_free_cells']  = innorow[9]
                    tmp_stats['ibuf_cell_count']  = innorow[12]
                elif line.startswith('Ibuf: size '):
                    # Ibuf: size 1, free list len 4634, seg size 4636,
                    tmp_stats['ibuf_used_cells']  = innorow[2]
                    tmp_stats['ibuf_free_cells']  = innorow[6]
                    tmp_stats['ibuf_cell_count']  = innorow[9]
                    if 'merges' in line:
                        tmp_stats['ibuf_merges']  = innorow[10]
                elif ', delete mark ' in line and prev_line.startswith('merged operations:'):
                    # Output of show engine innodb status has changed in 5.5
                    # merged operations:
                    # insert 593983, delete mark 387006, delete 73092
                    tmp_stats['ibuf_inserts'] = innorow[1]
                    tmp_stats['ibuf_merged']  = innorow[1] + innorow[4] + innorow[6]
                elif ' merged recs, ' in line:
                    # 19817685 inserts, 19817684 merged recs, 3552620 merges
                    tmp_stats['ibuf_inserts'] = innorow[0]
                    tmp_stats['ibuf_merged']  = innorow[2]
                    tmp_stats['ibuf_merges']  = innorow[5]
                elif line.startswith('Hash table size '):
                    # In some versions of InnoDB, the used cells is omitted.
                    # Hash table size 4425293, used cells 4229064, ....
                    # Hash table size 57374437, node heap has 72964 buffer(s) <-- no used cells
                    tmp_stats['hash_index_cells_total'] = innorow[3]
                    if 'used cells' in line:
                        tmp_stats['hash_index_cells_used'] = innorow[6]
                    else:
                        tmp_stats['hash_index_cells_used'] = 0
                    # LOG
                elif " log i/o's done, " in line:
                    # 3430041 log i/o's done, 17.44 log i/o's/second
                    # 520835887 log i/o's done, 17.28 log i/o's/second, 518724686 syncs, 2980893 checkpoints
                    # TODO: graph syncs and checkpoints
                    tmp_stats['log_writes'] = innorow[0]
                elif " pending log writes, " in line:
                    # 0 pending log writes, 0 pending chkp writes
                    tmp_stats['pending_log_writes']  = innorow[0]
                    tmp_stats['pending_chkp_writes'] = innorow[4]
                elif line.startswith( "Log sequence number"):
                    # This number is NOT printed in hex in InnoDB plugin.
                    # Log sequence number 13093949495856 //plugin
                    # Log sequence number 125 3934414864 //normal
                    if len(innorow) > 4:
                        tmp_stats['log_bytes_written'] = self.make_bigint(innorow[3], innorow[4])
                    else:
                        tmp_stats['log_bytes_written'] = innorow[3]
                elif line.startswith( "Log flushed up to"):
                    # This number is NOT printed in hex in InnoDB plugin.
                    # Log flushed up to   13093948219327
                    # Log flushed up to   125 3934414864
                    if len(innorow) >  5:
                        tmp_stats['log_bytes_flushed'] = self.make_bigint(innorow[4], innorow[5])
                    else:
                        tmp_stats['log_bytes_flushed'] = innorow[4]
                elif line.startswith( "Last checkpoint at"):
                    # Last checkpoint at  125 3934293461
                    if len(innorow) > 4:
                        tmp_stats['last_checkpoint'] = self.make_bigint(innorow[3], innorow[4])
                    else:
                        tmp_stats['last_checkpoint'] = innorow[3]
                    # BUFFER POOL AND MEMORY
                elif line.startswith( "Total memory allocated") and 'in additional pool' in line:
                    # Total memory allocated 29642194944; in additional pool allocated 0
                    tmp_stats['total_mem_alloc']       = innorow[3]
                    tmp_stats['additional_pool_alloc'] = innorow[8]
                elif line.startswith( 'Adaptive hash index '):
                    #   Adaptive hash index 1538240664     (186998824 + 1351241840)
                    tmp_stats['adaptive_hash_memory'] = innorow[3]
                elif line.startswith( 'Page hash           '):
                    #   Page hash           11688584
                    tmp_stats['page_hash_memory'] = innorow[2]
                elif line.startswith( 'Dictionary cache    '):
                    #   Dictionary cache    145525560      (140250984 + 5274576)
                    tmp_stats['dictionary_cache_memory'] = innorow[2]
                elif line.startswith( 'File system         '):
                    #   File system         313848         (82672 + 231176)
                    tmp_stats['file_system_memory'] = innorow[2]
                elif line.startswith( 'Lock system         '):
                    #   Lock system         29232616       (29219368 + 13248)
                    tmp_stats['lock_system_memory'] = innorow[2]
                elif line.startswith( 'Recovery system     '):
                    #   Recovery system     0      (0 + 0)
                    tmp_stats['recovery_system_memory'] = innorow[2]
                elif line.startswith( 'Threads             '):
                    #   Threads             409336         (406936 + 2400)
                    tmp_stats['thread_hash_memory'] = innorow[1]
                elif line.startswith( 'innodb_io_pattern   '):
                    #   innodb_io_pattern   0      (0 + 0)
                    tmp_stats['innodb_io_pattern_memory'] = innorow[1]
                elif line.startswith( "Buffer pool size ") and not line.startswith( "Buffer pool size bytes"):
                    # The " " after size is necessary to avoid matching the wrong line:
                    # Buffer pool size        1769471
                    # Buffer pool size, bytes 28991012864
                    tmp_stats['pool_size'] = innorow[3]
                elif line.startswith( "Free buffers"):
                    # Free buffers            0
                    tmp_stats['free_pages'] = innorow[2]
                elif line.startswith( "Database pages"):
                    # Database pages          1696503
                    tmp_stats['database_pages'] = innorow[2]
                elif line.startswith( "Modified db pages"):
                    # Modified db pages       160602
                    tmp_stats['modified_pages'] = innorow[3]
                elif line.startswith( "Pages read ahead"):
                    # Must do this BEFORE the next test, otherwise it'll get fooled by this
                    # line from the new plugin (see samples/innodb-015.txt):
                    # Pages read ahead 0.00/s, evicted without access 0.06/s
                    # TODO: No-op for now, see issue 134.
                    tmp_stats['empty'] = ''
                elif line.startswith( "Pages read"):
                    # Pages read 15240822, created 1770238, written 21705836
                    tmp_stats['pages_read']    = innorow[2]
                    tmp_stats['pages_created'] = innorow[4]
                    tmp_stats['pages_written'] = innorow[6]
                    # ROW OPERATIONS
                elif line.startswith( 'Number of rows inserted'):
                    # Number of rows inserted 50678311, updated 66425915, deleted 20605903, read 454561562
                    tmp_stats['rows_inserted'] = innorow[4]
                    tmp_stats['rows_updated']  = innorow[6]
                    tmp_stats['rows_deleted']  = innorow[8]
                    tmp_stats['rows_read']     = innorow[10]
                elif " queries inside InnoDB, " in line:
                    # 0 queries inside InnoDB, 0 queries in queue
                    tmp_stats['queries_inside'] = innorow[0]
                    tmp_stats['queries_queued'] = innorow[4]
            prev_line = line
        return tmp_stats.items()

    @staticmethod
    def increment(stats, value, increment):
        if value in stats:
            stats[value] += increment
        else:
            stats[value] = increment
        return stats

    @staticmethod
    def make_bigint(hi, lo = None):
        if lo == 0:
            return int("0x" + hi, 0)
        else:
            if hi == None:
                hi = 0
            if lo == None:
                lo = 0

        return (hi * 4294967296) + lo

    def run(self):
        """ Run forever """
        
        if not self.connection:
            """ Initial connection setup """
            self.setup_connection()

        while self.run:
            if self.die_on_max_reconnect and self.reconnect_attempt >= self.max_reconnect:
                raise ThreadMySQLMaxReconnectException

            if not self.connection.open:
                self.reconnect_attempt += 1
                print('Attempting reconnect #{0}...'.format(self.reconnect_attempt))
                self.setup_connection()
                continue

            self._run()

