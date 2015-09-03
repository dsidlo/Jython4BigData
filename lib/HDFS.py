#!/usr/local/bin/jython27 --hadoop --
'''
HDFS objects for hdfs file manipulation.
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

from Properties  import *
from PagingWindow import *

from org.apache.hadoop.conf             import Configuration, Configured
from org.apache.hadoop.fs               import FileSystem, FSDataInputStream, FSDataOutputStream, Path
from org.apache.hadoop.fs               import FsShell, FileUtil
from org.apache.hadoop.fs.permission    import FsPermission
from org.apache.log4j                   import Logger, Level

from java.io       import InputStreamReader, BufferedReader
from java.util     import HashMap
from java.io       import BufferedReader,InputStreamReader
from java.util.zip import GZIPInputStream
from java.lang     import Exception as JException

import unittest, string, socket, datetime, re, sys

# To enable the python debugger...
# Add the following function to break into debugging at that point
# in the code: pdb.set_trace()
import pdb

''' =================================================================
    Class for creating a handle to the Hdfs file system.
'''
class Hdfs():
    def __init__(self, hdfsCluster):
        self.logger = Logger.getLogger("Hdfs")
        # self.logger.setLevel(Level.DEBUG)

        coreSite = "/etc/hadoop/conf/core-site.xml"
        hdfsSite = "/etc/hadoop/conf/hdfs-site.xml"
        hdfsCluster = hdfsCluster
        self.cHdfs = Configuration()
        self.cHdfs.addResource(Path(coreSite))
        self.cHdfs.addResource(Path(hdfsSite))
        self.cHdfs.set("fs.defaultFS", hdfsCluster)
        self.fileSystem = FileSystem.get(self.cHdfs)
        self.fileUtil = FileUtil()

class TestHdfs(unittest.TestCase):
    def testHdfs (self):
        # Replik8db Properties...
        Replik8dbProps = Properties('conf/replik8db.properties') 
        # Replik8db HDFS Cluster...
        Replik8dbHdfsCluster = Replik8dbProps.getProperty('cluster.hdfs')

        hd = Hdfs(Replik8dbHdfsCluster);
        self.assert_(hd.fileSystem is not None)

''' =================================================================
    Create a class for moving files in HDFS
'''
class HdfsUtil():
    def __init__(self, hdfsCluster):
        self.logger = Logger.getLogger("HdfsUtil")
        # self.logger.setLevel(Level.DEBUG)

        self.hdfs = Hdfs(hdfsCluster)
        self.fsHd = self.hdfs.fileSystem

    def ls(self, fpath):
        ''' Returns a list of all files in a given dir.
            This file list can be very long and take lots of memory.
            Use lsIterator or lsFileIterator instead to minimize
            memory usage.
        '''
        p = Path(fpath)
        self.fileList = self.fsHd.listStatus(p)
        return self.fileList

    def lsIterator(self, fpath):
        ''' Returns an iterator that returns files and dirs
            w/in a given dir path (no recursion).
        '''
        p = Path(fpath)
        self.lsListIterator = self.fsHd.listLocatedStatus(p)
        return self.lsListIterator

    def lsFileIterator(self, fpath, recurse = False):
        ''' Returns an iterator that returns files (only)
            w/in a given dir path (w/the option for recursion).
        '''
        p = Path(fpath)
        self.fileListIterator = self.fsHd.listFiles(p, recurse)
        return self.fileListIterator

    def lsFileStatIterator(self, fpath, stat = 'mtime',
                           buffSize = 100,
                           rewind = False,
                           recurse = False,
                           findRange = False,
                           _lastVal = None):

        # print ( "lsFileStatIterator(fpath[{}], stat[{}], buffSize[{}]," +
        #          "rewind[{}], recurse[{}], findRange[{}]," +
        #          " _lastVal[{}])" ).format( fpath,
        #                                     stat,
        #                                     buffSize,
        #                                     rewind,
        #                                     recurse,
        #                                     findRange,
        #                                     _lastVal)

        ''' lfsi = lsFileStatIterator( filePath
                                       stat = "[atime|mtime|owner|group|length|path]",
                                       buffSize = <N>, {Size of list return on each scan of the dir}
                                       recurse = False,
                                       rewind = False )

            Returns an iterator that returns files (only) w/in a given dir path
            (w/the option for recursion),
            and includes the option of prioritizing files by a given
            fileStat (atime (access time), mtime (modification time),
            owner, group, length, or path.
            This iterator fill a buffer-list with the next files to process.
            It executes a full directory scan on each iteration until the either
            the buffer list is full or all files have been read into the buffer-list.
            The rewind option (when set to true) restarts the directory read from the
            start of the directory on every buffer refresh. This is useful in cases
            where you need to process files with a priority on handling the oldest first,
            before moving it out of that directory.
            _lastVal is used internally by this method to keep track of the last value
            read into the buffer-list for the default case where rewind is set to False.

            Usage:

            '''

        pwFiles = PagingWindow(limitSize = buffSize, preserveSmallest = True)

        # print "===> fpath[{}]".format(fpath)
        # Create an HDFS file iterator to get the dir listing (which is returned in a random order).
        lsi = self.lsFileIterator(fpath, recurse = recurse)

        self.minRange = None
        self.maxRange = None

        dirFileCount = 0
        # Process all files in the dir...
        while(lsi.hasNext()):
            it = lsi.next()
            fp = it.getPath()
            dirFileCount += 1
            # Create a sortable value <fs-fileStat:fp-filePath>
            if stat == 'mtime':
                fs = it.getModificationTime()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'atime':
                fs = it.getAccessTime()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'blocksize':
                fs = it.getBlockSize()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'len':
                fs = it.getLen()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'group':
                fs = it.getGroup()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'owner':
                fs = it.getOwner()
                fkey = "{}.:.{}".format(fs, fp)
            elif stat == 'path':
                fkey = "{}".format(fp)
            else:
                fkey = None

            if findRange and (fkey is not None):
                if (self.minRange is None) or (self.minRange > fkey):
                    self.minRange = fkey
                if (self.maxRange is None) or (self.maxRange < fkey):
                    self.maxRange = fkey

            # Skip over values already seen, unless rewind is set.
            if (_lastVal is not None) and (rewind is False):
                if _lastVal >= fkey:
                    continue

            # print "..> fkey:[{}]".format(fkey)
            if fkey is not None:
                pwFiles.add(fkey, it)

        # print "xx -> pwFiles.size[{}] hash.size[{}]".format( pwFiles.size(),
        #                                                      pwFiles.hash.size() )

        fsi = FileStatIterator(self, 
                               fpath, 
                               stat, 
                               pwFiles, 
                               buffSize = buffSize,
                               recurse = recurse, 
                               rewind = rewind )

        # Place the range values into the return iterator object
        fsi.minRange     = self.minRange
        fsi.maxRange     = self.maxRange
        fsi.dirFileCount = dirFileCount

        return fsi

    def touch(self, fpath):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            os = self.fsHd.create(fp)
            os.close()
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.touch({}): ex[{}]".format(fpath, ex))

    def mkdir(self, fpath, perms=755):
        try:
            p = Path(fpath)
            permObj = FsPermission("{}".format(perms))
            retVal = self.fsHd.mkdirs(p, permObj)
            if retVal == False:
                self.logger.error("HdfsUtil.mkdir({}): Failed to create dir.".format(fpath, ex))
                
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.mkdir({}): ex[{}]".format(fpath, ex))

    def exists(self, fpath):
        try:
            # Create an empty file on HDFS
            sp = []
            fileList = self.fsHd.globStatus(Path(fpath))
            if ((fileList is None) or (fileList.__len__() == 0)):
                # Emit and error: No files found for srcfPath
                return False

            return True

        except JException as ex:
            self.logger.error("Exception in HdfsUtil.exists({}): ex[{}]".format(fpath, ex))

    def isFile(self, fpath):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            return self.fsHd.isFile(fp)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.isFile({}): ex[{}]".format(fpath, ex))

    def isDir(self, fpath):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            return self.fsHd.isDirectory(fp)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.isDir({}): ex[{}]".format(fpath, ex))

    def getFileStat(self, fpath):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            return self.fsHd.getFileStatus(fp)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.getFileStat({}): ex[{}]".format(fpath, ex))

    def setOwner(self, fpath, user, group):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            return self.fsHd.setOwner(fp, user, group)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.setOwner({}): ex[{}]".format(fpath, ex))

    def setPerms(self, fpath, perms):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            fsPerm = FsPermission(perms)
            return self.fsHd.setPerms(fp, perms)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.setPerms({}): ex[{}]".format(fpath, ex))

    def setRep(self, fpath, replication):
        try:
            # Create an empty file on HDFS
            fp = Path (fpath)
            return self.fsHd.setReplication(fp, replication)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.setReplication({}): ex[{}]".format(fpath, ex))

    def rm(self, fpath, **kwargs):
        try:
            sp = []
            fileList = self.fsHd.globStatus(Path(fpath))
            if (fileList is None):
                # self.logger.warn("No Files found in: [{}]".format(fpath))
                return

            if 'recurse' in kwargs:
                recurse = kwargs['recurse']
            else:
                recurse = False

            for sfp in fileList:
                self.fsHd.delete(sfp.getPath(), recurse)


        except JException as ex:
            self.logger.error("Exception in HdfsUtil.rm({}): ex[{}]".format(fpath, ex))

    def mv(self, srcfpath, trgfpath):
        try:
            sp = Path(srcfpath)
            tp = Path(trgfpath)
            #  Needs work...
            self.fsHd.rename(sp, tp)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.mv({}): ex[{}]".format(fpath, ex))

    def cp(self, srcfpath, trgfpath):
        "Copy data within the current HDFS."
        try:
            sp = []
            fileList = self.fsHd.globStatus(Path(srcfpath))
            if ((fileList is None) or (fileList.__len__() == 0)):
                # Emit and error: No files found for srcfPath
                None
            for sfp in fileList:
                sp.append(sfp.getPath())

            sfs = FileSystem.newInstance(self.hdfs.cHdfs)
            tp = Path(trgfpath)
            tfs = FileSystem.newInstance(self.hdfs.cHdfs)
            delSrc = False
            overWrite = True
            self.hdfs.fileUtil.copy(sfs, sp, tfs, tp, delSrc, overWrite, self.hdfs.cHdfs)
        except JException as ex:
            self.logger.error("Exception in HdfsUtil.cp({} -> {}): ex[{}]".format(srcfpath, trgfpath, ex))


    ''' Returns a Java BufferedReader that can be used to
        read data from fPath (HDFS-file-path)
    '''
    def openRead(self, fPath):
        fpHdfs = Path(fPath); 
        fsInput = self.fsHd.open(fpHdfs); 

        reader = None
        pat = r'.*\.gz'
        match = re.search(pat, hst) 
        if match == None:
            reader = BufferedReader(InputStreamReader(fsInput)) 
        else:
            # The file stream is in GZip format... 
            reader = BufferedReader(InputStreamReader(GZIPInputStream(fsInput))) 

        return reader

    ''' Returns an org.apache.hadoop.fs.FSDataOutputStream
        that can be used to output to the fPath (file-path) in HDFS.

        offest = 0;
        while ((bytesRead = in.read(buffer)) > 0) {
            out.write(buffer, offset, bytesRead);
            offest = offest + bytesRead
        }

    '''
    def openWrite(self, fPath):
        fp = Path(fPath); 
        fsOutput = self.fsHd.create(fp); 

        return fsOutput

class FileStatIterator():
    ''' FileStatIterator is the iterator that is returned by the lsFileStatIterator() method.
        lsFileStatIterator() will fill a sorted PagingWindow array with stat:filePath elements,
        and a Java Map with Key:path -> Value:fileSystemObject pairs.
        FileStatIterator, iterates through the sorted PagingWindow and returns corresponding
        fileSystemObjects to the caller. When the sorted Limited list is empty, a call
        to lsFileStatIterator() is made to refresh the sorte PagingWindow and filesystemObject
        Java Map.
    '''
    def __init__(self, hdfsUtil, dirPath, stat, pwFiles,
                 buffSize = 100, recurse = False, rewind = False):
        self.logger = Logger.getLogger("FileStatIterator")
        # self.logger.setLevel(Level.DEBUG)

        self.hu       = hdfsUtil
        self.dirPath  = dirPath
        self.stat     = stat
        self.pwFiles  = pwFiles
        self.buffSize = buffSize
        self.recurse  = recurse
        self.rewind   = rewind

        if pwFiles.size() > 0:
            self.firstVal = pwFiles[0]
            self.lastVal  = pwFiles[pwFiles.size() - 1]
        else:
            self.firstVal = None
            self.lastVal  = None

    def __iter__(self):
        return self

    def hasNext(self):

        # print "--> hasNext[{}]".format(self.pwFiles.size())

        if self.pwFiles.size() > 0:
            return True
        else:
            return False

    def _next(self):
        # Pops the stat:filePath out the sorted PagingWindow
        if self.hasNext():
            keyVal = self.pwFiles.pop(0)
            if not self.hasNext():
                # Pulls more data into the sorted PagingWindow buffer once empty.
                self.logger.debug( ("--> refreshing pwFiles list buffSize[{}]" +
                                    " rewind[{}] lastVal[{}]").format(self.buffSize,
                                                                      self.rewind,
                                                                      self.lastVal) )

                # When we give up out last item, we try to refresh the files list.
                hufsi = self.hu.lsFileStatIterator(self.dirPath,
                                                   stat = self.stat,
                                                   buffSize = self.buffSize,
                                                   rewind = self.rewind,
                                                   recurse = self.recurse,
                                                   findRange = False,
                                                   _lastVal = self.lastVal )

                self.logger.debug( ("--> hufsi pwFiles.size[{}]" +
                                    " pwFiles.hash.size[{}]" +
                                    " lastVal[{}]" ).format(hufsi.pwFiles.size(),
                                                            hufsi.pwFiles.hash.size(),
                                                            hufsi.lastVal) )
                self.pwFiles = hufsi.pwFiles
                self.lastVal = hufsi.lastVal

            # Return the poped item.
            return keyVal
        else:
            # No more data in dir.
            return None

    def next(self):
        # self.logger.debug( ("-(next)-0-> self pwFiles.size[{}]" +
        #                     " pwFiles.hash.size[{}]" ).format(self.pwFiles.size(),
        #                                                       self.pwFiles.hash.size()) )

        # Pop the stat:filePath out the sorted PagingWindow
        keyVal = self._next()

        # self.logger.debug( ("-(next)-1-> self pwFiles.size[{}]" +
        #                     " pwFiles.hash.size[{}]" ).format(self.pwFiles.size(),
        #                                                       self.pwFiles.hash.size()) )

        # Return the fileSystemObject
        return keyVal

    def size(self):
        return self.pwFiles.size()

class TestHdfsUtil(unittest.TestCase):
    def setUp(self):
        # Replik8db Properties...
        Replik8dbProps = Properties('conf/replik8db.properties') 
        # Replik8db HDFS Cluster...
        Replik8dbHdfsCluster = Replik8dbProps.getProperty('cluster.hdfs')

        self.logger = Logger.getLogger("TestHdfsUtil")
        # self.logger.setLevel(Level.DEBUG)

        self.hu = HdfsUtil(Replik8dbHdfsCluster)
 
    def testHdfsUtil_ls (self):
        files = self.hu.ls("/data/")
        self.assert_(len(files) > 0)

    ''' *** These test all need to be able to run completely on thier own.
        Should create their own files and dirs to test functionality, and should not
        rely on an exist set of data.
    '''

    def testHdfsUtil_lsIterator (self):
        fileCnt = 0
        fit = self.hu.lsIterator("/data/")
        while(fit.hasNext()):
            path = fit.next().getPath()
            # print "-> [{}]".format(path.getName())
            fileCnt = fileCnt + 1
            if fileCnt > 10:
                break
        self.assert_(fileCnt > 0)

    def testHdfsUtil_lsFileIterator (self):
        fileCnt = 0
        fit = self.hu.lsFileIterator("/data/", recurse = True)
        while(fit.hasNext()):
            path = fit.next().getPath()
            # print "-=> [{}]".format(path.getName())
            fileCnt = fileCnt + 1
            if fileCnt > 10:
                break
        self.assert_(fileCnt > 0)

    def testHdfsUtil_lsFileStatIterator_0next(self):
        testDirPath1 = '/data/event_files_to_process/'
        s = time.time()
        fsi = self.hu.lsFileStatIterator(testDirPath1,
                                         stat = 'mtime',
                                         buffSize = 10,
                                         findRange = True )

        # print "\nminRange[{}]".format(fsi.minRange)
        # print   "maxRange[{}]".format(fsi.maxRange)
        # print   "pwFiles.size[{}]".format(fsi.size())
        # print   "pwFiles.hash.size[{}]".format(fsi.pwFiles.hash.size())
        # print "Secs to read dir:[{}]".format(time.time() - s)

        self.assertTrue(fsi.pwFiles.size() == 10)
        self.assertTrue(fsi.pwFiles.hash.size() == 10)
        cnt = 0
        while fsi.hasNext():
            cnt += 1
            kv = fsi._next()
            # print "\n[0next] -({})-> fv[{}]".format(cnt, kv[0])
            if cnt >= 25:
                break
        None

    def testHdfsUtil_lsFileStatIterator_next(self):
        testDirPath1 = '/data/event_files_to_process/'
        if not self.hu.exists(testDirPath1):
            testDirPath1 = '/data/event_files_to_process-2014-08-27'

        s = time.time()
        bufferSize = 500
        fsi = self.hu.lsFileStatIterator(testDirPath1,
                                         stat = 'mtime',
                                         buffSize = bufferSize)
        # print "pwFiles.size[{}] <= bufferSize[{}]".format(fsi.pwFiles.size(), bufferSize)
        self.assertTrue(fsi.pwFiles.size() <= bufferSize)
        # print "Secs to read dir:[{}]".format(time.time() - s)
        cnt = 0
        while fsi.hasNext():
            cnt += 1
            kv = fsi.next()
            if cnt == 1:
                # print the first record
                # print "\n[next] -({})-> kv[{}]".format(cnt, kv[1].getPath())
                None
            if cnt >= 1500:
                # print the last record
                # print "[next] -({})-> kv[{}]".format(cnt, kv[1].getPath())
                break

        # print "Secs to read dir:[{}]".format(time.time() - s)

    def testHdfsUtil_lsFileStatIterator_rewind(self):
        testDirPath1 = '/data/event_files_to_process/'
        if not self.hu.exists(testDirPath1):
            testDirPath1 = '/data/event_files_to_process-2014-08-27'

        s = time.time()
        bufferSize = 500
        fsi = self.hu.lsFileStatIterator(testDirPath1, 
                                         stat = 'mtime', 
                                         buffSize = bufferSize, 
                                         rewind = True)
        # print "pwFiles.size[{}] <= bufferSize[{}]".format(fsi.pwFiles.size(), bufferSize)
        self.assertTrue(fsi.pwFiles.size() <= bufferSize)
        cnt = 0
        while fsi.hasNext():
            cnt += 1
            kv = fsi.next()
            if cnt == 1:
                # print the first record
                # print "\n[rewind] -({})-> kv[{}]".format(cnt, kv[0])
                None
            if cnt >= 1500:
                # print the last record
                # print "[rewind] -({})-> kv[{}]".format(cnt, kv[0])
                break

        # print "Secs to read dir:[{}]".format(time.time() - s)

    def testHdfsUtil_touch_exists_rm(self):
        tmpFilePath = '/tmp/__akjsdklfjslfjkd_'
        self.hu.touch(tmpFilePath)
        self.assert_(self.hu.exists(tmpFilePath))
        self.hu.rm(tmpFilePath)
        self.assert_(not self.hu.exists(tmpFilePath))
        
    def testHdfsUtil_mkdir(self):
        testDir = "/tmp/__testDir-mviqjthvpihjweqr/"
        self.hu.mkdir(testDir, 777)
        self.assert_( self.hu.exists(testDir))
        self.assert_( self.hu.isDir(testDir))
        self.hu.rm(testDir, recurse = True)

    def testHdfsUtil_mv(self):
        tmpFilePath1 = '/tmp/_1_oqiuweriuqwrues_'
        tmpFilePath2 = '/tmp/_2_oqiuweriuqwrues_'
        self.hu.rm(tmpFilePath1)
        self.hu.rm(tmpFilePath2)
        self.assert_(not self.hu.exists(tmpFilePath1))
        self.hu.touch(tmpFilePath1)
        self.assert_(self.hu.exists(tmpFilePath1))
        self.hu.mv(tmpFilePath1, tmpFilePath2)
        self.assert_(self.hu.exists(tmpFilePath2))
        self.hu.rm(tmpFilePath2)
        self.assert_(not self.hu.exists(tmpFilePath2))

    # This is here to test test-methods with a prefix
    # that is not 'test'.
    def integrationExample(self):
        print "This is a test with a different prefix!"
        self.assert_(False)

    def testIsFile(self):
        tmpFilePath1  = '/tmp/_1_iopoetrertcvcv_/'
        self.hu.touch(tmpFilePath1)
        self.assert_(self.hu.isFile(tmpFilePath1))
        self.hu.rm(tmpFilePath1)

    def testIsDir(self):
        tmpDirPath1  = '/tmp/_1_khcsetgvujikmnkj_/'
        self.hu.mkdir(tmpDirPath1)
        self.assert_(self.hu.isDir(tmpDirPath1))
        self.hu.rm(tmpDirPath1)

    def testGetFileStat(self):
        tmpFilePath1 = '/tmp/_1_sakjhsnzxcvnjkd_'
        self.hu.touch(tmpFilePath1)
        self.assert_(self.hu.getFileStat(tmpFilePath1).getLen() == 0)
        self.hu.rm(tmpFilePath1)

    def testCpRm(self):
        tmpFilePath1 = '/tmp/_1_akjsdklfjslfjkd_'
        tmpFilePath2 = '/tmp/_2_akjsdklfjslfjkd_'
        tmpDirPath1  = '/tmp/_3_akjsdklfjslfjkd_/'
        tmpFilePath3 = tmpDirPath1 + '_1_akjsdklfjslfjkd_'
        # self.logger.info("tmpFilePath3:[{}]".format(tmpFilePath3))
        self.hu.rm(tmpFilePath3)
        # ================================================= 
        # Test cp (single file)...
        self.hu.touch(tmpFilePath1)
        self.hu.cp(tmpFilePath1, tmpFilePath2)
        self.assert_(self.hu.exists(tmpFilePath2))
        # Test cp file into dir...
        self.hu.mkdir(tmpDirPath1, 777)
        self.hu.cp(tmpFilePath1, tmpDirPath1)
        self.assert_(self.hu.exists(tmpFilePath3))
        # Cleanup...
        self.hu.rm(tmpFilePath1)
        self.assert_(not self.hu.exists(tmpFilePath1))
        self.hu.rm(tmpFilePath2)
        # self.hu.rm(tmpDirPath1)
        # self.assert_(self.hu.exists(tmpDirPath1))
        self.hu.rm(tmpDirPath1, recurse = True)
        self.assert_(not self.hu.exists(tmpDirPath1))

    def testCpGlobRm(self):
        tmpFilePath1 = '/tmp/_1_akjsdklfjslfjkd32_'
        tmpFilePath2 = '/tmp/_2_akjsdklfjslfjkd32_'
        tmpGlobPath1 = '/tmp/_[12]_akjsdklfjslfjkd3[2]_'
        tmpDirPath1  = '/tmp/_3_akjsdklfjslfjkd32_/'
        tmpGlobPath2 = tmpDirPath1 + '_*akjsdklfjslfjkd3[2]_'
        # self.logger.info("tmpGlobPath1:[{}]".format(tmpGlobPath1))
        self.hu.rm(tmpGlobPath1, recurse = True)
        # ================================================= 
        # Test cp (file globbing)...
        self.hu.touch(tmpFilePath1)
        self.hu.cp(tmpFilePath1, tmpFilePath2)
        self.assert_(self.hu.exists(tmpFilePath2))
        # Test cp file into dir...
        self.hu.mkdir(tmpDirPath1, 777)
        # pdb.set_trace()
        self.hu.cp(tmpGlobPath1, tmpDirPath1)
        self.assert_(self.hu.exists(tmpGlobPath2))
        # Cleanup...
        self.hu.rm(tmpFilePath1)
        self.hu.rm(tmpFilePath2)
        self.assert_(not self.hu.exists(tmpFilePath1))
        self.hu.rm(tmpGlobPath2)
        self.assert_(not self.hu.exists(tmpGlobPath2))
        self.hu.rm(tmpDirPath1, recurse = True)
        self.assert_(not self.hu.exists(tmpDirPath1))

    def testSetOwner(self):
        # self.assert_(False)
        None

    def testSetPerms(self):
        # self.assert_(False)
        None

    def testsetReplication(self):
        # self.assert_(False)
        None


''' =================================================================
    Class for HDFS file...
    Swallows all lines of a file.
'''
class HdfsSwallowFile():

    def __init__(self, hdfsCluster, fpath):
        self.hdfs = Hdfs(hdfsCluster)
        self.fsHd = self.hdfs.fileSystem

        fpHdfs = Path(fpath);
        fsInput = self.fsHd.open(fpHdfs);
        # The file has text so we want to use read the input stream via the BufferedReader.
        reader = BufferedReader(InputStreamReader(fsInput))
        self.lineCount = 0
        self.lines = []
        line = reader.readLine()
        while line is not None:
            # print line
            self.lines.append(line)
            self.lineCount = self.lineCount + 1
            if ((self.lineCount % 1000) == 0):
                print self.lineCount
            line = reader.readLine()

class TestHdfsSwallowFile(unittest.TestCase):
    def testHdfsSwallowFile(self):
        # Replik8db Properties...
        Replik8dbProps = Properties('conf/replik8db.properties') 
        # Replik8db HDFS Cluster...
        Replik8dbHdfsCluster = Replik8dbProps.getProperty('cluster.hdfs')

        testFile = "/data/UnitTestData/chicle/07-29-2014-eventdata-999037270"
        swf = HdfsSwallowFile(Replik8dbHdfsCluster, testFile)
        # for ln in swf.lines[0:3:1]:
        #    print "--> "+ln
        # print "Line Count: [%d]" % swf.lineCount
        self.assert_(swf.lineCount == 83)


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
