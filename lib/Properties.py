#!/usr/local/bin/jython27 --hadoop --
"""
This module defines several functions to ease interfacing with Java code.
"""

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

from types import *

from java.lang   import *
from java.util   import Properties as jProperties
from java.io     import BufferedInputStream, FileInputStream

from org.apache.log4j import Logger, Level

import unittest, sys


# only expose these
# __all__ = ['loadProperties', 'getProperty', 
#            'mapToJava', 'mapFromJava', 'parseArgs']

class Properties():
    def __init__(self, propertyFile):
        self.props = None
        if (propertyFile is not None):
            self.props = self.loadProperties(propertyFile)

    def loadProperties (self, source):
        """ Load a Java properties file into a Dictionary. """
        result = {}

        # name provided, use file
        if type(source) == type(''):
            source = FileInputStream(source)

        bis = BufferedInputStream(source)
        props = jProperties()
        props.load(bis) 
        bis.close()
        for key in props.keySet().iterator():
            result[key] = props.get(key)
        return result

    def parseBoolString(self, theString):
        return theString[0].upper() == 'T'

    def getProperty (self, name, default=None):
        """ Gets a property. """
        return self.props.get(name, default)

    def getIntProperty(self, name, default=None):
        x = self.getProperty(name)
        if x == None:
            return default
        else:
            return int(x) 

    def getBoolProperty(self, name, default=False):
        x = self.getProperty(name)
        if x == None:
            return default
        else:
            return self.parseBoolString(x) 


class TestProperties(unittest.TestCase):
    def setUp(self):
        self.logger = Logger.getLogger("Properties")
        # self.logger.setLevel(Level.DEBUG)
        self.logger.debug("running test: TestEsNode")

    def testProps(self):
        props = Properties("./conf/TestProperties.properties")
        self.assert_(props.getIntProperty('debug') == 1)
        self.assert_(props.getIntProperty('xxx', 333) == 333)
        self.assert_(props.getProperty('error.level') == 'ERROR')
        self.assert_(props.getProperty('warn.level', 'WARN') == 'WARN')
        self.assert_(props.getBoolProperty('now.is.the.time') == True)
        self.assert_(props.getBoolProperty('to.go.to.h3ll', False) == False)
        self.assert_(props.getBoolProperty('false.test') == False)



if __name__ == '__main__':

    # rootLogger = Logger.getLogger("root").setLevel(Level.DEBUG)

    if (len(sys.argv) == 1):
        # ----------------
        # Run all tests...
        # ----------------
        unittest.main()
    
        # ---------------------
        # Run specific tests...
        # ---------------------
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestEsLogger)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestHdfsUtil)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestHostBuckets)
        # unittest.TextTestRunner(verbosity=2).run(suite)

