#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest, os
from mysql_statsd.preprocessors import InnoDBPreprocessor

class InnoDBPreprocessorTest(unittest.TestCase):
    def test_values_read_from_vanilla_install(self):
        """
        For this mysqld version in default setup::

            $ /usr/sbin/mysqld --version
            /usr/sbin/mysqld  Ver 5.5.41-0+wheezy1 for debian-linux-gnu on x86_64 ((Debian))

        A basic coverage test.
        """
        fixture = os.path.join(os.path.dirname(__file__), 'fixtures',
                               'show-innodb-status-5.5-vanilla')
        row = open(fixture, 'rb').read()

        processor = InnoDBPreprocessor()
        processed = processor.process([('InnoDB', '', row)])
        expected = {
                'additional_pool_alloc': '0',
                'current_transactions': 1,
                'database_pages': '142',
                'empty': '',
                'free_pages': '8049',
                'hash_index_cells_total': '276671',
                'hash_index_cells_used': 0,
                'history_list': '0',
                'ibuf_cell_count': '2',
                'ibuf_free_cells': '0',
                'ibuf_merges': '0',
                'ibuf_used_cells': '1',
                'innodb_transactions': 1282,
                'last_checkpoint': '1595685',
                'log_bytes_flushed': '1595685',
                'log_bytes_written': '1595685',
                'modified_pages': '0',
                'os_waits': '0',
                'pages_created': '0',
                'pages_read': '142',
                'pages_written': '1',
                'pending_buf_pool_flushes': '0',
                'pending_log_flushes': '0',
                'pending_normal_aio_reads': '0',
                'pending_normal_aio_writes': '0',
                'pool_size': '8191',
                'read_views': '1',
                'rows_deleted': '0',
                'rows_inserted': '0',
                'rows_read': '0',
                'rows_updated': '0',
                'spin_rounds': '0',
                'spin_waits': '0',
                'total_mem_alloc': '137363456',
                'unpurged_txns': 1282, }
        self.assertEquals(expected, dict(processed))

if __name__ == "__main__":
    unittest.main()
