#!/usr/local/bin/jython27 --hbase --
'''
HBase object for table/row insert read.
'''

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import re, fileinput, sys, gzip, unittest

from os import listdir

from org.apache.hadoop.hbase        import HBaseConfiguration, HTableDescriptor, \
                                           HColumnDescriptor, HConstants
from org.apache.hadoop.hbase.client import HBaseAdmin, HTable, Get, Put, Delete, Result
from org.apache.log4j               import Logger, Level

from java.lang                      import System
from java.lang                      import Exception as JException


class HBaseTable(object):
    '''
    Represents ah HBase table and operation that can be performed on an HBase table.
    '''
    def __init__(self, name, conf, admin):
        self.logger = Logger.getLogger("HBaseTable")
        # self.logger.setLevel(Level.DEBUG)
        self.conf = conf
        self.admin = admin
        self.name = name
        self._table = None

    def row(self, rowId):
        if not self._table:
            self._table = HTable(self.conf, self.name)
        return HBaseRow(self._table, rowId)

    def scan(self, start_row=None, end_row=None, filter=None):
        '''
        Read the table including and between start and end rows
        applying the given filter.
        '''
        if not self._table:
            self._table = HTable(self.conf, self.name)
        sc = None
        if start_row and filter:
            sc = Scan(start_row, filter)
        elif start_row and end_row:
            sc = Scan(start_row, end_row)
        elif start_row:
            sc = Scan(start_row)
        else:
            sc = Scan()
        s = self._table.getScanner(sc)
	while True:
            r = s.next()
	    if r is None:
                raise StopIteration()
            yield r

class HBaseRow(object):
    def __init__(self, table, rowid): 
        self.logger = Logger.getLogger("HBaseRow")
        # self.logger.setLevel(Level.DEBUG)
        # Must be a table object
        self.table = table
        # Must be a rowid
        self.rowid = rowid
        self.rowPut = Put(self.rowid)

    def put(self, family, column, value):
        ''' 
        For the current Table.
        Set the value of a given column-family. column.
        '''
        self.rowPut.add(family, column, value)

    def tablePut (self):
        self.table.put(self.rowPut)

    def get(self, family, column):
        ''' 
        For the current Table.
        get the value of a given column-family. column.
        '''
        row = self.table.get(Get(self.rowid))
        val = row.getValue(family, column)
        if val is not None:
            # Not sure if we would always want to do this.
            # -dgs
            return val.tostring()


class TestHBase (unittest.TestCase):
    def testHBase (self):
        conf = HBaseConfiguration()
        tablename = "Trees"
        admin = HBaseAdmin(conf)
        if not admin.tableExists(tablename):
            sys.stderr.write('\nERROR - Table name not found:  ' + tablename + '\n\n')
            exit(1)
        # From org.apache.hadoop.hbase.client, create an HTable.
        table = HTable(conf, tablename)
        print 'connection successful'

        # Create the HBaseTable Object.
        hbtable = HBaseTable (tablename, conf, admin)

        # Read a single row from the table.
        rowId = '005430000000000038000163'

        # Put a row into the table.
        row = hbtable.row(rowId)
        row.put('f', 'name', "xxxxxxxxxxxxxxxxxxxxxxx")
        row.tablePut()

        # Read the row back from the table.
        print hbtable.row(rowId).get('f','name')

        # Mark the row as deleted.


def main ():
    None

if __name__ == '__main__': 
    if (len(sys.argv) == 1):
        # ----------------
        # Run all tests...
        # ----------------
        unittest.main()

        # ---------------------
        # Run specific tests...
        # ---------------------
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestEventFileToIndex)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestHdfsUtil)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestShellReturn)
        # unittest.TextTestRunner(verbosity=2).run(suite)

    else:
        if (sys.argv[1] != 'integration'):
            # ------------------------------------------------
            # How to run tests that have a different prefix...
            # ------------------------------------------------
            runner = unittest.TextTestRunner()
            mySuite = unittest.makeSuite(TestHdfsUtil, prefix='integration')
            runner.run(mySuite)

