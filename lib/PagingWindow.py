#!/usr/local/bin/jython27 --
# -*- coding: utf-8 -*-
'''
PagingWindow.py

This object is a generalized pager that is used by
HDFS.py to page though dirs containing tens of thousands of files.
Without the use of this Pager, listing a really large directory
on your local machine can cause it to run out of memory.


...ramblings about this code...


This is a list that is limited to a given size.

It has a fixed size.

It has min-key and a max-key values.

You can limit on the min-key, the max-key or both.

When you limit on the min-key, that means that if you add a key that is
smaller than the curren min-key, and the list is at its max size,
the key will be thrown away. A key larger than the min-key will cause the
current min-key to be removed and the new larger key to be inserted.

When you limit on the max-key, that means that if you add a key that is
larger than the current max-key, and the list is at its max size,
it will be thrown away. A key smaller than the max-key will cause the
current max-key to be removed and the new smaller key to be inserted.
In other works, limiting on max-key allows the LimitList object to
take on larger keys at the expense of throwing away smaller keys in order
to limit the size of the list.

When both the min-key and the max-key limits are enforced, only the key
values that can fit into the list size, and whos values are between the
current min and max key values are kept. When the max list size is met,
min and max key values may change as new values force out the old.
In other works, limiting on max-key allows the LimitList object to
take on smaller keys at the expense of throwing away larger keys in order
to limit the size of the list.

There is a special condition that occurs when both to min and max limits are
set Once the list had reaced its mx size. The issues is the choice of which
limit to removed, in order to insert the next key that falls within the
range of min and max. In this case, by default, we make a random choice. 
The choice of which side to drop is determined by this function chooseSide.
- .chooseSide(_value)
This method can be overriden, as there are many possiblities
to examine based on your use case. Possibilities include, making the choice
based on the centre element (odd list length), or the center 2 elements 
(event list length). Or, the value of the centroids. Or the positional nature
of the new element based on min, and max elements, etc...

The initial use of this class is to iterate through sets of the oldest
files within an HDFS directory. In this use case, we only enforce the
limit on the max-key (newest file creation date). Thus, the max-key
value might get larger as long as the list has not met its max size.
Once the list hits its max size, the max-key value might get smaller
if younger files are found and push inserted into the list, while the oldest
files are removed from of the list.
The HDFS file list iterators are used in production programs when file
lists can get so large that it can be a burden to only the full file
list in memory.
The HDFS file list iterators don't sort the file list in any way.
We will use the HDFS file list iterator to scan through all of the
files in a given directory, and we will use the PagingWindow class to
hold on to only N of the oldest files found.
After we have passed over the whole HDFS directory, adding files into
the PagingWindow object, the object will only contain the oldest set of files.

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

from UserList      import *
from java.util     import HashMap

import random, time, unittest

class PagingWindow(UserList):
    def __init__(self, **kwargs):
        # The list is contained w/in self.data
        UserList.__init__(self)
        
        self.limitSize = 0      # Unbounded
        self.limitMin = False   # unbounded
        self.limitMax = False   # unbounded
        
        self.minElmIndx = -1
        self.minElmVal  = None
        self.maxElmIndx = -1
        self.maxElmVal  = None

        # Create a hash map for storing an object as well.
        self.hash = HashMap()

        opts = { 'limitSize' : self.optLimitSize,
                 'limitMin'  : self.optLimitMin,
                 'limitMax'  : self.optLimitMax,
                 
                 'preserveSmallest'  : self.optPreserveSmallest,
                 'preserveLargest'   : self.optPreserveLargest,
        }

        # Process each optional argumen.
        for k in kwargs.keys():
            optFunc = opts[k]
            if optFunc is None:
                raise LookupError("Option [k] is not supported by the PagingWindow class.")
            else:
                optFunc(kwargs[k])
        
        random.seed(time.time())
        
    def optLimitSize(self, _size):
        if type(_size).__name__ != "int":
            raise ValueError("limitSize parameter must be type int. Got type [{}].".format(type(_size).__name__))
        self.limitSize = _size
        
    def optLimitMin(self, _min):
        if type(_min).__name__ != "bool":
            raise ValueError("limitMin parameter must be type bool.")
        self.limitMin = _min

    def optLimitMax(self, _max):
        if type(_max).__name__ != "bool":
            raise ValueError("limitMax parameter must be type bool.")
        self.limitMax = _max
        
    def optPreserveSmallest(self, _small):
        if type(_small).__name__ != "bool":
            raise ValueError("preserveSmallest parameter must be type bool.")
        if _small:
            self.limitMin = False
            self.limitMax = True

    def optPreserveLargest(self, _large):
        if type(_large).__name__ != "bool":
            raise ValueError("preserveLargest parameter must be type bool.")
        if _large:
            self.limitMin = True
            self.limitMax = False


    def add(self, _key, _value = None):

        # print "==> value[{}] limitSize[{}]".format(_key, self.limitSize)
        # print "==> data.__len__[%d]" % self.data.__len__()

        dataLen = self.data.__len__()

        if dataLen < self.limitSize:
            ''' Here we add to the list when the list had not reached its
                max size.
            '''
            # print "..> appeding to data: [%s]" % _key
            self.data.append(_key)

            if _value is not None:
                # print " ++> added _value[{}]".format(_value)
                self.hash.put(_key, _value)

            # We should remove the sort on every insert.
            # Use sortedcontainers instead.
            self.data.sort()
        else:
            # print "..> not appending to data: [%s]" % _key

            insertMinOk = True
            insertMaxOk = True

            if self.limitMin:
                ''' If the new value is greater than the current minElement,
                    we may need to remove the current minElement to make room
                    for the new value.
                '''
                if self.data.__len__ > 0:
                    # The minElmIndx is always 0,
                    # unless the array has no data.
                    self.minElmIndx = 0
                else:
                    self.minElmIndx = -1
                if self.minElmIndx >= 0:
                    self.minElmVal = self.data[self.minElmIndx]
                    if _key < self.minElmVal:
                        insertMinOk = False
                        
            if self.limitMax:
                ''' If the new value is smaller than the current maxElement,
                    we may need to remove the current maxElement to make room
                    for the new value.
                '''
                self.maxElmIndx = self.data.__len__() - 1
                if self.maxElmIndx > 0:
                    self.maxElmVal = self.data[self.maxElmIndx]
                    if _key > self.maxElmVal:
                        insertMaxOk = False

            if self.limitMin and self.limitMax:
                     ''' Handle the case where it is ok to insert for either
                         case of limitMin and limitMax
                     '''
                     if insertMinOk and insertMaxOk:
                         # choseSize() may be a custom function that gets passed in.
                         side = self.choseSide(_key)
                         if side == 0:
                             raise AssertionError("chooseSide() should not return 0 as a result")
                         if side < 0:
                             if self.minElmVal is not None:
                                 self.data.remove(self.minElmVal)
                                 if self.hash.containsKey(self.minElmVal):
                                     self.hash.remove(self.minElmVal)
                         if side > 0:
                             if self.maxElmVal is not None:
                                 self.data.remove(self.maxElmVal)
                                 if self.hash.containsKey(self.maxElmVal):
                                     self.hash.remove(self.maxElmVal)
            else:
                if self.limitMin:
                    if insertMinOk:
                        if self.minElmVal is not None:
                            self.data.remove(self.minElmVal)
                            if self.hash.containsKey(self.maxElmVal):
                                self.hash.remove(self.maxElmVal)
                    else:
                        if self.data.__len__() + 1 > self.limitSize:
                            return False

                if self.limitMax:
                    if insertMaxOk:
                        if self.maxElmVal is not None:
                            self.data.remove(self.maxElmVal)
                            if self.hash.containsKey(self.maxElmVal):
                                self.hash.remove(self.maxElmVal)
                    else:
                        if self.data.__len__() + 1 > self.limitSize:
                            return False

            self.data.append(_key)
            if _value is not None:
                # print " ++> added _value[{}]".format(_value)
                self.hash.put(_key, _value)

            # We should remove the sort on every insert.
            # Possibly use sortedcontainers instead.
            self.data.sort()
            # Return True when a value is added
            return True

    def pop(self, indx):
        # Pop the 0 item from the list
        _key = super(UserList, self).pop(indx)

        # By default, return the key.
        retVal = _key

        # But, if the key has a corresponding value in the hash...
        if self.hash.containsKey(_key):
            # return the hash...
            retVal = [ _key, self.hash.get(_key) ]
            # and removed the object from the hash
            self.hash.remove(_key)

        return retVal

    def chooseSide(_key):
        r = random.getrandbits(1)

        if (r == 0):
            return -1
            
        if (r == 1):
            return 1

    def size(self):
        return self.data.__len__()

class TestPagingWindow(unittest.TestCase):
    def testLimitMin_001(self):
        ll = PagingWindow(limitSize = 10, limitMin = True, limitMax = True)
        self.assertTrue(ll is not None)
        self.assertTrue(ll.limitSize == 10)
        self.assertTrue(ll.limitMin is True)
        self.assertTrue(ll.limitMax is True)

    def testLimitMin_002(self):
        ll = PagingWindow(limitSize = 20, limitMin = False, limitMax = False)
        self.assertTrue(ll is not None)
        self.assertTrue(ll.limitSize == 20)
        self.assertTrue(ll.limitMin is False)
        self.assertTrue(ll.limitMax is False)

    def testLimitMin_003(self):
        # print "******> testLimitMin_003 <*****"
        ll = PagingWindow(limitSize = 5, limitMin = True, limitMax = False)
        # print "--> ll:{}".format(ll)
        for i in range(10, 0, -1):
             ll.add('%03d' % i )

        # print "==> ll:{}".format(ll)
        self.assertTrue(ll[0] == '006')
        self.assertTrue(ll[4] == '010')
        self.assertTrue(ll.__len__() == 5)

    def testLimitMin_004(self):
        # print "******> testLimitMin_004 <*****"
        ll = PagingWindow(limitSize = 5, preserveLargest = True)
        # print "--> ll:{}".format(ll)
        for i in range(1, 11, 1):
             ll.add('%03d' % i )

        # print "==> ll:{}".format(ll)
        self.assertTrue(ll[0] == '006')
        self.assertTrue(ll[4] == '010')
        self.assertTrue(ll.__len__() == 5)

    def testLimitMax_001(self):
        # print "******> testLimitMax_001 <*****"
        ll = PagingWindow(limitSize = 5, limitMin = False, limitMax = True)
        # print "--> ll:{}".format(ll)
        for i in range(10, 0, -1):
             ll.add('%03d' % i )

        # print "==> ll:{}".format(ll)
        self.assertTrue(ll[0] == '001')
        self.assertTrue(ll[4] == '005')
        self.assertTrue(ll.__len__() == 5)
             
    def testLimitMax_002(self):
        # print "******> testLimitMax_002 <*****"
        ll = PagingWindow(limitSize = 5, preserveSmallest = True)
        # print "--> ll:{}".format(ll)
        for i in range(1, 11, 1):
             ll.add('%03d' % i )

        # print "==> ll:{}".format(ll)
        self.assertTrue(ll[0] == '001')
        self.assertTrue(ll[4] == '005')
        self.assertTrue(ll.__len__() == 5)

    def testLimitMax_003(self):
        # print "******> testLimitMax_003 <*****"
        ll = PagingWindow(limitSize = 5, preserveSmallest = True)
        # print "--> ll:{}".format(ll)
        x = [ 4,3,6,2,1,8,7,9,10,5 ]
        for i in x:
             ll.add('%03d' % i )

        # print "==> ll:{}".format(ll)
        self.assertTrue(ll[0] == '001')
        self.assertTrue(ll[4] == '005')
        self.assertTrue(ll.__len__() == 5)


    def testLimitMax(self):
        None
        
if __name__ == '__main__': 
     # ---------------- 
     # Run all tests... 
     # ---------------- 
     unittest.main() 
     
 
