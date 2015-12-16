from interface import Preprocessor
import re


class InnoDBPreprocessor(Preprocessor):
    _INNO_LINE = re.compile(r'\s+')
    _DIGIT_LINE = re.compile(r'\d+\.*\d*')
    tmp_stats = {}
    txn_seen = 0
    prev_line = ''

    @staticmethod
    def increment(stats, value, increment):
        if value in stats:
            stats[value] += increment
        else:
            stats[value] = increment
        return stats

    @staticmethod
    def make_bigint(hi, lo=None):
        if lo == 0:
            return int("0x" + hi, 0)
        else:
            if hi is None:
                hi = 0
            if lo is None:
                lo = 0

        return (hi * 4294967296) + lo

    def clear_variables(self):
        self.tmp_stats = {}
        self.txn_seen = 0
        self.prev_line = ''

    def process(self, rows):
        # The show engine innodb status is basically a bunch of sections, so we'll try to separate them in chunks
        chunks = {'junk': []}
        current_chunk = 'junk'
        next_chunk = False
        oldest_view = False

        self.clear_variables()
        for row in rows:
            innoblob = row[2].replace(',', '').replace(';', '').replace('/s', '').split('\n')
            for line in innoblob:
                # All chunks start with more than three dashes. Only the individual innodb bufferpools have three dashes
                if line.startswith('---OLDEST VIEW---'):
                    oldest_view = True
                if line.startswith('----'):
                    # First time we see more than four dashes have to record the new chunk
                    if next_chunk == False and oldest_view == False:
                        next_chunk = True
                    else:
                    # Second time we see them we just have recorded the chunk
                        next_chunk = False
                        oldest_view = False
                elif next_chunk == True: 
                    # Record the chunkname and initialize the array
                    current_chunk = line
                    chunks[current_chunk] = []
                else:
                    # Or else we just stuff the line in the chunk
                    chunks[current_chunk].append(line)
        for chunk in chunks:
            # For now let's skip individual buffer pool info not have it mess up our stats when enabled
            if chunk != 'INDIVIDUAL BUFFER POOL INFO':
                for line in chunks[chunk]:
                    self.process_line(line)

        # Process the individual buffer pool
        bufferpool = 'bufferpool_0.'
        for line in chunks.get('INDIVIDUAL BUFFER POOL INFO', []):
            # Buffer pool stats are preceded by:
            # ---BUFFER POOL X
            if line.startswith('---'):
                innorow = self._INNO_LINE.split(line)
                bufferpool = 'bufferpool_' + innorow[2] + '.'
            else:
                self.process_individual_bufferpools(line, bufferpool)

        return self.tmp_stats.items()

    def process_individual_bufferpools(self,line,bufferpool):
        innorow = self._INNO_LINE.split(line)
        if line.startswith("Buffer pool size ") and not line.startswith("Buffer pool size bytes"):
            # The " " after size is necessary to avoid matching the wrong line:
            # Buffer pool size        1769471
            # Buffer pool size, bytes 28991012864
            self.tmp_stats[bufferpool + 'pool_size'] = innorow[3]
        elif line.startswith("Buffer pool size bytes"):
            self.tmp_stats[bufferpool + 'pool_size_bytes'] = innorow[4]
        elif line.startswith("Free buffers"):
            # Free buffers            0
            self.tmp_stats[bufferpool + 'free_pages'] = innorow[2]
        elif line.startswith("Database pages"):
            # Database pages          1696503
            self.tmp_stats[bufferpool + 'database_pages'] = innorow[2]
        elif line.startswith("Old database pages"):
            # Database pages          1696503
            self.tmp_stats[bufferpool + 'old_database_pages'] = innorow[3]
        elif line.startswith("Modified db pages"):
            # Modified db pages       160602
            self.tmp_stats[bufferpool + 'modified_pages'] = innorow[3]
        elif line.startswith("Pending reads"):
            # Pending reads       0
            self.tmp_stats[bufferpool + 'pending_reads'] = innorow[2]
        elif line.startswith("Pending writes"):
            # Pending writes: LRU 0, flush list 0, single page 0
            self.tmp_stats[bufferpool + 'pending_writes_lru'] = self._DIGIT_LINE.findall(innorow[3])[0]
            self.tmp_stats[bufferpool + 'pending_writes_flush_list'] = self._DIGIT_LINE.findall(innorow[6])[0]
            self.tmp_stats[bufferpool + 'pending_writes_single_page'] = innorow[9]
        elif line.startswith("Pages made young"):
            # Pages made young 290, not young 0
            self.tmp_stats[bufferpool + 'pages_made_young'] = innorow[3]
            self.tmp_stats[bufferpool + 'pages_not_young'] = innorow[6]
        elif 'youngs/s' in line:
            # 0.50 youngs/s, 0.00 non-youngs/s
            self.tmp_stats[bufferpool + 'pages_made_young_ps'] = innorow[0]
            self.tmp_stats[bufferpool + 'pages_not_young_ps'] = innorow[2]
        elif line.startswith("Pages read ahead"):
            # Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
            self.tmp_stats[bufferpool + 'pages_read_ahead'] = self._DIGIT_LINE.findall(innorow[3])[0]
            self.tmp_stats[bufferpool + 'pages_read_evicted'] = self._DIGIT_LINE.findall(innorow[7])[0]
            self.tmp_stats[bufferpool + 'pages_read_random'] = self._DIGIT_LINE.findall(innorow[11])[0]
        elif line.startswith("Pages read"):
            # Pages read 88, created 66596, written 221669
            self.tmp_stats[bufferpool + 'pages_read'] = innorow[2]
            self.tmp_stats[bufferpool + 'pages_created'] = innorow[4]
            self.tmp_stats[bufferpool + 'pages_written'] = innorow[6]
        elif 'reads' in line and 'creates' in line:
            # 0.00 reads/s, 40.76 creates/s, 137.97 writes/s
            self.tmp_stats[bufferpool + 'pages_read_ps'] = innorow[0]
            self.tmp_stats[bufferpool + 'pages_created_ps'] = innorow[2]
            self.tmp_stats[bufferpool + 'pages_written_ps'] = innorow[4]
        elif line.startswith("Buffer pool hit rate"):
            # Buffer pool hit rate 1000 / 1000, young-making rate 0 / 1000 not 0 / 1000
            self.tmp_stats[bufferpool + 'buffer_pool_hit_total'] = self._DIGIT_LINE.findall(innorow[6])[0]
            self.tmp_stats[bufferpool + 'buffer_pool_hits'] = innorow[4]
            self.tmp_stats[bufferpool + 'buffer_pool_young'] = innorow[9]
            self.tmp_stats[bufferpool + 'buffer_pool_not_young'] = innorow[13]
        elif line.startswith("LRU len:"):
            # LRU len: 21176, unzip_LRU len: 0
            self.tmp_stats[bufferpool + 'lru_len'] = self._DIGIT_LINE.findall(innorow[2])[0]
            self.tmp_stats[bufferpool + 'lru_unzip'] = innorow[5]
        elif line.startswith("I/O sum"):
            # I/O sum[29174]:cur[285], unzip sum[0]:cur[0]
            self.tmp_stats[bufferpool + 'io_sum'] = self._DIGIT_LINE.findall(innorow[1])[0]
            self.tmp_stats[bufferpool + 'io_sum_cur'] = self._DIGIT_LINE.findall(innorow[1])[1]
            self.tmp_stats[bufferpool + 'io_unzip'] = self._DIGIT_LINE.findall(innorow[3])[0]
            self.tmp_stats[bufferpool + 'io_unzip_cur'] = self._DIGIT_LINE.findall(innorow[3])[0]






    def process_line(self, line):
        innorow = self._INNO_LINE.split(line)
        if line.startswith('Mutex spin waits'):
           # Mutex spin waits 79626940, rounds 157459864, OS waits 698719
           # Mutex spin waits 0, rounds 247280272495, OS waits 316513438
            self.tmp_stats['spin_waits'] = innorow[3]
            self.tmp_stats['spin_rounds'] = innorow[5]
            self.tmp_stats['os_waits'] = innorow[8]

        elif line.startswith('RW-shared spins') and ';' in line:
            # RW-shared spins 3859028, OS waits 2100750; RW-excl spins 4641946, OS waits 1530310
            self.tmp_stats['spin_waits'] = innorow[2]
            self.tmp_stats['spin_waits'] = innorow[8]
            self.tmp_stats['os_waits'] = innorow[5]
            self.tmp_stats['os_waits'] += innorow[11]

        elif line.startswith('RW-shared spins') and '; RW-excl spins' in line:
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-shared spins 604733, rounds 8107431, OS waits 241268
            self.tmp_stats['spin_waits'] = innorow[2]
            self.tmp_stats['os_waits'] = innorow[7]

        elif line.startswith('RW-excl spins'):
            # Post 5.5.17 SHOW ENGINE INNODB STATUS syntax
            # RW-excl spins 604733, rounds 8107431, OS waits 241268
            self.tmp_stats['spin_waits'] = innorow[2]
            self.tmp_stats['os_waits'] = innorow[7]

        elif 'seconds the semaphore:' in line:
            # --Thread 907205 has waited at handler/ha_innodb.cc line 7156 for 1.00 seconds the semaphore:
            self.tmp_stats = self.increment(self.tmp_stats, 'innodb_sem_waits', 1)
            if 'innodb_sem_wait_time_ms' in self.tmp_stats:
                self.tmp_stats['innodb_sem_wait_time_ms'] = float(self.tmp_stats['innodb_sem_wait_time_ms']) + float(innorow[9]) * 1000
            else:
                self.tmp_stats['innodb_sem_wait_time_ms'] = float(innorow[9]) * 1000

        # TRANSACTIONS
        elif line.startswith('Trx id counter'):
            # The beginning of the TRANSACTIONS section: start counting
            # transactions
            # Trx id counter 0 1170664159
            # Trx id counter 861B144C
            if len(innorow) == 4:
                innorow.append(0)
            self.tmp_stats['innodb_transactions'] = self.make_bigint(innorow[3], innorow[4])
            self.txn_seen = 1

        elif line.startswith('Purge done for trx'):
            # Purge done for trx's n:o < 0 1170663853 undo n:o < 0 0
            # Purge done for trx's n:o < 861B135D undo n:o < 0
            if innorow[7] == 'undo':
                innorow[7] = 0
            self.tmp_stats['unpurged_txns'] = int(self.tmp_stats['innodb_transactions']) - self.make_bigint(innorow[6], innorow[7])

        elif line.startswith('History list length'):
            # History list length 132
            self.tmp_stats['history_list'] = innorow[3]

        elif self.txn_seen == 1 and line.startswith('---TRANSACTION'):
            # ---TRANSACTION 0, not started, process no 13510, OS thread id 1170446656
            self.tmp_stats = self.increment(self.tmp_stats, 'current_transactions', 1)
            if 'ACTIVE' in line:
                self.tmp_stats = self.increment(self.tmp_stats, 'active_transactions', 1)

        elif self.txn_seen == 1 and line.startswith('------- TRX HAS BEEN'):
            # ------- TRX HAS BEEN WAITING 32 SEC FOR THIS LOCK TO BE GRANTED:
            self.tmp_stats = self.increment(self.tmp_stats, 'innodb_lock_wait_secs', innorow[5])

        elif 'read views open inside InnoDB' in line:
            # 1 read views open inside InnoDB
            self.tmp_stats['read_views'] = innorow[0]

        elif line.startswith('mysql tables in use'):
            # mysql tables in use 2, locked 2
            self.tmp_stats = self.increment(self.tmp_stats, 'innodb_tables_in_use', innorow[4])
            self.tmp_stats = self.increment(self.tmp_stats, 'innodb_locked_tables', innorow[6])

        elif self.txn_seen == 1 and 'lock struct(s)' in line:
            # 23 lock struct(s), heap size 3024, undo log entries 27
            # LOCK WAIT 12 lock struct(s), heap size 3024, undo log entries 5
            # LOCK WAIT 2 lock struct(s), heap size 368
            if line.startswith('LOCK WAIT'):
                self.tmp_stats = self.increment(self.tmp_stats, 'innodb_lock_structs', innorow[2])
                self.tmp_stats = self.increment(self.tmp_stats, 'locked_transactions', 1)
            else:
                self.tmp_stats = self.increment(self.tmp_stats, 'innodb_lock_structs', innorow[0])

        # FILE I/O
        elif ' OS file reads, ' in line:
            # 8782182 OS file reads, 15635445 OS file writes, 947800 OS fsyncs
            self.tmp_stats['file_reads'] = innorow[0]
            self.tmp_stats['file_writes'] = innorow[4]
            self.tmp_stats['file_fsyncs'] = innorow[8]

        elif line.startswith('Pending normal aio reads:'):
            # Pending normal aio reads: 0, aio writes: 0,
            self.tmp_stats['pending_normal_aio_reads'] = innorow[4]
            self.tmp_stats['pending_normal_aio_writes'] = innorow[7]

        elif line.startswith('ibuf aio reads'):
            #  ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
            self.tmp_stats['pending_ibuf_aio_reads'] = innorow[3]
            self.tmp_stats['pending_aio_log_ios'] = innorow[6]
            self.tmp_stats['pending_aio_sync_ios'] = innorow[9]

        elif line.startswith('Pending flushes (fsync)'):
            # Pending flushes (fsync) log: 0; buffer pool: 0
            self.tmp_stats['pending_log_flushes'] = innorow[4]
            self.tmp_stats['pending_buf_pool_flushes'] = innorow[7]

        elif line.startswith('Ibuf for space 0: size '):
            # Older InnoDB code seemed to be ready for an ibuf per tablespace.  It
            # had two lines in the output.  Newer has just one line, see below.
            # Ibuf for space 0: size 1, free list len 887, seg size 889, is not empty
            # Ibuf for space 0: size 1, free list len 887, seg size 889,
            self.tmp_stats['ibuf_used_cells'] = innorow[5]
            self.tmp_stats['ibuf_free_cells'] = innorow[9]
            self.tmp_stats['ibuf_cell_count'] = innorow[12]

        elif line.startswith('Ibuf: size '):
            # Ibuf: size 1, free list len 4634, seg size 4636,
            self.tmp_stats['ibuf_used_cells'] = innorow[2]
            self.tmp_stats['ibuf_free_cells'] = innorow[6]
            self.tmp_stats['ibuf_cell_count'] = innorow[9]
            if 'merges' in line:
                self.tmp_stats['ibuf_merges'] = innorow[10]

        elif ', delete mark ' in line and self.prev_line.startswith('merged operations:'):
            # Output of show engine innodb status has changed in 5.5
            # merged operations:
            # insert 593983, delete mark 387006, delete 73092
            self.tmp_stats['ibuf_inserts'] = innorow[1]
            self.tmp_stats['ibuf_merged'] = innorow[1] + innorow[4] + innorow[6]

        elif ' merged recs, ' in line:
            # 19817685 inserts, 19817684 merged recs, 3552620 merges
            self.tmp_stats['ibuf_inserts'] = innorow[0]
            self.tmp_stats['ibuf_merged'] = innorow[2]
            self.tmp_stats['ibuf_merges'] = innorow[5]

        elif line.startswith('Hash table size '):
            # In some versions of InnoDB, the used cells is omitted.
            # Hash table size 4425293, used cells 4229064, ....
            # Hash table size 57374437, node heap has 72964 buffer(s) <-- no used cells
            self.tmp_stats['hash_index_cells_total'] = innorow[3]
            if 'used cells' in line:
                self.tmp_stats['hash_index_cells_used'] = innorow[6]
            else:
                self.tmp_stats['hash_index_cells_used'] = 0

        # LOG
        elif " log i/o's done, " in line:
            # 3430041 log i/o's done, 17.44 log i/o's/second
            # 520835887 log i/o's done, 17.28 log i/o's/second, 518724686 syncs, 2980893 checkpoints
            # TODO: graph syncs and checkpoints
            self.tmp_stats['log_writes'] = innorow[0]

        elif " pending log writes, " in line:
            # 0 pending log writes, 0 pending chkp writes
            self.tmp_stats['pending_log_writes'] = innorow[0]
            self.tmp_stats['pending_chkp_writes'] = innorow[4]

        elif line.startswith("Log sequence number"):
            # This number is NOT printed in hex in InnoDB plugin.
            # Log sequence number 13093949495856 //plugin
            # Log sequence number 125 3934414864 //normal
            if len(innorow) > 4:
                self.tmp_stats['log_bytes_written'] = self.make_bigint(innorow[3], innorow[4])
            else:
                self.tmp_stats['log_bytes_written'] = innorow[3]

        elif line.startswith("Log flushed up to"):
            # This number is NOT printed in hex in InnoDB plugin.
            # Log flushed up to   13093948219327
            # Log flushed up to   125 3934414864
            if len(innorow) > 5:
                self.tmp_stats['log_bytes_flushed'] = self.make_bigint(innorow[4], innorow[5])
            else:
                self.tmp_stats['log_bytes_flushed'] = innorow[4]

        elif line.startswith("Last checkpoint at"):
            # Last checkpoint at  125 3934293461
            if len(innorow) > 4:
                self.tmp_stats['last_checkpoint'] = self.make_bigint(innorow[3], innorow[4])
            else:
                self.tmp_stats['last_checkpoint'] = innorow[3]

        # BUFFER POOL AND MEMORY
        elif line.startswith("Total memory allocated") and 'in additional pool' in line:
            # Total memory allocated 29642194944; in additional pool allocated 0
            self.tmp_stats['total_mem_alloc'] = innorow[3]
            self.tmp_stats['additional_pool_alloc'] = innorow[8]

        elif line.startswith('Adaptive hash index '):
            #   Adaptive hash index 1538240664     (186998824 + 1351241840)
            self.tmp_stats['adaptive_hash_memory'] = innorow[3]

        elif line.startswith('Page hash           '):
            #   Page hash           11688584
            self.tmp_stats['page_hash_memory'] = innorow[2]

        elif line.startswith('Dictionary cache    '):
            #   Dictionary cache    145525560      (140250984 + 5274576)
            self.tmp_stats['dictionary_cache_memory'] = innorow[2]

        elif line.startswith('File system         '):
            #   File system         313848         (82672 + 231176)
            self.tmp_stats['file_system_memory'] = innorow[2]

        elif line.startswith('Lock system         '):
            #   Lock system         29232616       (29219368 + 13248)
            self.tmp_stats['lock_system_memory'] = innorow[2]

        elif line.startswith('Recovery system     '):
            #   Recovery system     0      (0 + 0)
            self.tmp_stats['recovery_system_memory'] = innorow[2]

        elif line.startswith('Threads             '):
            #   Threads             409336         (406936 + 2400)
            self.tmp_stats['thread_hash_memory'] = innorow[1]

        elif line.startswith('innodb_io_pattern   '):
            #   innodb_io_pattern   0      (0 + 0)
            self.tmp_stats['innodb_io_pattern_memory'] = innorow[1]

        elif line.startswith("Buffer pool size ") and not line.startswith("Buffer pool size bytes"):
            # The " " after size is necessary to avoid matching the wrong line:
            # Buffer pool size        1769471
            # Buffer pool size, bytes 28991012864
            self.tmp_stats['pool_size'] = innorow[3]

        elif line.startswith("Free buffers"):
            # Free buffers            0
            self.tmp_stats['free_pages'] = innorow[2]

        elif line.startswith("Database pages"):
            # Database pages          1696503
            self.tmp_stats['database_pages'] = innorow[2]

        elif line.startswith("Modified db pages"):
            # Modified db pages       160602
            self.tmp_stats['modified_pages'] = innorow[3]

        elif line.startswith("Pages read ahead"):
            # Must do this BEFORE the next test, otherwise it'll get fooled by this
            # line from the new plugin (see samples/innodb-015.txt):
            # Pages read ahead 0.00/s, evicted without access 0.06/s
            # TODO: No-op for now, see issue 134.
            self.tmp_stats['empty'] = ''

        elif line.startswith("Pages read"):
            # Pages read 15240822, created 1770238, written 21705836
            self.tmp_stats['pages_read'] = innorow[2]
            self.tmp_stats['pages_created'] = innorow[4]
            self.tmp_stats['pages_written'] = innorow[6]
            # ROW OPERATIONS

        elif line.startswith('Number of rows inserted'):
            # Number of rows inserted 50678311, updated 66425915, deleted 20605903, read 454561562
            self.tmp_stats['rows_inserted'] = innorow[4]
            self.tmp_stats['rows_updated'] = innorow[6]
            self.tmp_stats['rows_deleted'] = innorow[8]
            self.tmp_stats['rows_read'] = innorow[10]
        elif " queries inside InnoDB, " in line:
            # 0 queries inside InnoDB, 0 queries in queue
            self.tmp_stats['queries_inside'] = innorow[0]
            self.tmp_stats['queries_queued'] = innorow[4]
