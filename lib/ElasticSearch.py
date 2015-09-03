#!/usr/local/bin/jython27 --hadoop --elasticsearch --
# -- hadoop is needed to complete log4j initialization

from org.elasticsearch.node.NodeBuilder                                  import *
from org.elasticsearch.common.settings                                   import ImmutableSettings
from org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder import *

from Properties     import *
from BoonJson       import *
from WorkerThreads  import *
from datetime       import *

from java.util         import Map as JMap, HashMap, ArrayList
from java.text         import SimpleDateFormat
from org.apache.log4j  import Logger, Level
from org.boon.Boon     import *
from org.boon.json     import JsonFactory, JsonSerializerFactory

from threading import Thread, Lock, RLock, Condition, Semaphore, BoundedSemaphore, Event, Timer

import unittest, sys, time as pytime, Queue, socket

# To enable the python debugger...
# Add the following function to break into debugging at that point
# in the code: pdb.set_trace()
import pdb, traceback

# Chicle Properties...
ChicleProps = Properties('./conf/ElasticSearchTest.properties') 
# Testing or not testing... 
ChicleTesting = ChicleProps.getBoolProperty('esTest.testing') 
# Global list of hosts which will run the process... 
HostListCsv = ChicleProps.getBoolProperty('esTest.hosts') 
# Cluster to join...
ChicleCluster = ChicleProps.getProperty('esTest.cluster')
# Test Cluster to join...
ChicleTestCluster = ChicleProps.getProperty('esTest.test.cluster')
# Save Indexation  Failures to the BitBucket?
BitBktIndexationFailures = ChicleProps.getBoolProperty('esTest.bitbkt.indexation,failures', default = False)

''' =================================================================
    =================================================================
    TODO: 
    - Add Classes for Delete
    - Add configurable Retry Delay
    - Add configurable Index Failure handling (BitBucket Insertion)
    =================================================================
    =================================================================
'''

''' =================================================================
    Class for creating an ES client.
'''
class EsNode():
    def __init__(self, clusterName, unicastHosts, nodeName):
 
        self.logger = Logger.getLogger("EsNode")
        # self.logger.setLevel(Level.DEBUG)

        cluster = clusterName
        
        # Get this hosts IP address
        ip =  " ".join(socket.gethostbyaddr(socket.gethostname())[2])

        self.logger.info("Chicle joining cluster [{}] as node.name [{}] ip [{}]".format(cluster, nodeName, ip)) 

        esSets = ImmutableSettings.settingsBuilder()
        esSets = esSets.put('node.name', nodeName)
        esSets = esSets.put('client.transport.sniff', True)
        esSets = esSets.put("http.enabled", "false")
        esSets = esSets.put("transport.tcp.port", "9300-9400")
        esSets = esSets.put("discovery.zen.ping.multicast.enabled", "false")
        esSets = esSets.put("discovery.zen.ping.unicast.hosts", unicastHosts)
        esSets = esSets.put("network.host", ip)
        self.node = nodeBuilder().clusterName(cluster).client(True).settings(esSets).node()

        if (self.node is None):
            self.logger.error("Failed to create ESNode")

    # Create a client for this Node. 
    # We should be able to create multiple clients.
    def getClient(self):
        client = self.node.client()
        return client

    def close(self):
        self.node.close()
        self.logger.info("Closing EsNode.")

class TestEsNode(unittest.TestCase):
    def setUp(self):
        self.logger = Logger.getLogger("ElasticSearch.TestEsNode")
        # self.logger.setLevel(Level.DEBUG)

        self.logger.debug("running test: TestEsNode")
        
    def testEsNode(self):
        # Pull date from out test properties file...
        esTestProps    = Properties('./lib/ElasticSearchTest.properties')
        esTestCluster  = esTestProps.getProperty('esTest.cluster')
        esTestHosts    = esTestProps.getProperty('esTest.hosts')
        esTestNodeName = esTestProps.getProperty('esTest.node.name')
        esTestIndex    = esTestProps.getProperty('esTest.index')

        # Create the EsNode instance.
        esNc = EsNode(esTestCluster, esTestHosts, esTestNodeName)
        # Get a client instance from the EsNode instance.
        esClient = esNc.getClient()
        esNc.close()

        self.assert_(esClient is not None)


''' =================================================================
    Class for creating a EsBulkReqRetryItem for submittion to the esBulkReqRetryQueue
'''
class EsBulkReqRetryItem():
    def __init__(self, bulkReqRetry):
        self.bulkReqRetry = bulkReqRetry
        self.createdTime = pytime.time()
        self.queTime = pytime.time()
        self.retryActionsCount = 0
        self.batchNumber = 0

''' =================================================================
    Class for submitting a bulk request
'''
class EsBulkReq():
    def __init__(self, esClient,
                       esLogger, 
                       retrySeconds = 90,
                       bitBucket = None ):

        self.logger = Logger.getLogger("ElasticSearch.EsBulkReq")
        # self.logger.setLevel(Level.DEBUG)

        self.esLogger = esLogger

        self.esClient = esClient
        self.bulkReq = self.esClient.prepareBulk()
        self.reqCounter = 0
        self.currentRequests = ArrayList()

        # bitBucket is an object that implements a bitBucketSubmit() method.
        # BulkRequest Actions that fail, if not re-tryable, are submitted
        # to the bitBucket.
        self.bitBucket = bitBucket

        # Create the bulkRequestRetry Queue.
        # This is required for supporting bulkRequestRetries for
        # situations where new indexes are not created on time (w/in 30seconds).
        # This index is eventually created, but, in the mean time, we would lose
        # the data from the inserts that failed, unless, we can retry them after N seconds.
        self.bulkReqRetryQueue = Queue.PriorityQueue()
        self.bulkRetryBatchNumber = 0
        # This should be in a config file.
        self.bulkRetryDelaySeconds = retrySeconds

        # Needed for logging.
        self.threadName = 'EsBulkReq'

    ''' EsBulkRequest Add Action...
        add(request, indexName, typeMapName, jsonEventDoc)
    '''
    def add(self, req, indexName, typeMap, jsonEvent):
        self.reqCounter = self.reqCounter + 1
        self.bulkReq.add(req)

        pat = r'"log_event":'
        match = re.search(pat, jsonEvent)
        if match is not None:
            self.logger.info("A BitBucket event was submitted.")

        # We cache the original request and the source json event so that
        # we can reference it later during exception handling.
        cReq = [ req, indexName, typeMap, jsonEvent ]
        self.currentRequests.add(cReq)

    # Execute this bulkRequest.
    # Captures failed index/create actions for a delayed-retry-submission.
    # Retry is set to true if this instance of a bulk request execute is a "retry"
    def execute(self, retry=False):
        if (self.reqCounter > 0):
            # First we execute the bulkRequest in the bulkReq object (if any).
            self.logger.info("EsBulkReq Submitting [{}] actions retry[{}].".format(self.reqCounter, retry))
            self.bulkResp = self.bulkReq.execute().actionGet()

            if (self.bulkResp.hasFailures()):
                # Smart Bulk Request Response Handling...
                # There are cases where the bulk requests for items that require the creation of a new index, fails, because
                # the new indices' primary shards are not created within 30secs.
                # What I would like to do here, is to find action items that fail because of the above, save these action
                # items to a new bulk response object, which we will submit with a Nsec delay.
                # Seems like we'll need a bulkRequestRetry queue that we have to periodically check and process.
                indx = -1
                retryActionsCount = 0
                bulkReqRetry = None

                # Create Dics for failure stats.
                statsIdx = {}

                # Create Dics for retry stats.
                statsIdxRt = {}

                # Create Dics for retry stats.
                statsIdxBb = {}

                # Create a Dic to accumulate the bulk failure types.
                statsBulkFail = {}

                # Loop through BulkRequest Response Items and find actions that failed.
                for br in self.bulkResp.getItems():
                    indx = indx + 1

                    idxMapNm = self.currentRequests[indx][1] + "/" + self.currentRequests[indx][2]
                    if statsIdx.has_key(idxMapNm):
                        statsIdx[idxMapNm] += 1
                    else:
                        statsIdx[idxMapNm]  = 1

                    if br.failed:
                        if not retry:
                            self.logger.debug("BulkIndexFailure: [{}]".format(br.getFailureMessage()))

                            # Accumulate bulk failure stats.
                            brFailure = br.getFailureMessage()
                            if statsBulkFail.has_key(brFailure):
                                statsBulkFail[brFailure] += 1
                            else:
                                statsBulkFail[brFailure]  = 1

                            pat = r'.indices/create... nested. ProcessClusterEventTimeoutException.failed to process cluster event'
                            match = re.search(pat, br.getFailureMessage())
                            if (match != None):
                                # Create a new bulkReqRetry object if needed.
                                if bulkReqRetry is None:
                                    bulkReqRetry = EsBulkReq(self.esClient, self.esLogger, None, self.bitBucket)
                                    self.logger.info( "Created bulkReqRetry object." )

                                # Add failed request to bulkReqRetry
                                if (BitBktIndexationFailures):
                                    # bulkReqAction, indexName, typeMapName, jsonStrDoc
                                    bulkReqRetry.add(self.currentRequests[indx][0],
                                                     self.currentRequests[indx][1],
                                                     self.currentRequests[indx][2],
                                                     self.currentRequests[indx][3])

                                retryActionsCount = retryActionsCount + 1

                                # Accumulate Retry stats for indexNames/typeMap.
                                if statsIdxRt.has_key(idxMapNm):
                                    statsIdxRt[idxMapNm] += 1
                                else:
                                    statsIdxRt[idxMapNm]  = 1

                            else:
                                if self.bitBucket is not None:
                                    # Place the indexation failure into the bitBucket
                                    if (BitBktIndexationFailures):
                                        self.bitBucket.bitBucket( 2, self.currentRequests[indx][3],
                                                                  br.getFailureMessage())

                                        # Accumulate BitBucket stats for indexNames/typeMap.
                                        if statsIdxBb.has_key(idxMapNm):
                                            statsIdxBb[idxMapNm] += 1
                                        else:
                                            statsIdxRt[idxMapNm]  = 1

                                else:
                                    self.logger.warn("No BitBucket for failed BulkRequest action [{}]".format(self.currentRequests[indx][3]))

                        else:
                            # If a bulkReq Retry fails, just output an error message
                            self.logger.error("BulkIndexFailureOnRetry: [{}]".format(br.getFailureMessage()))
                            # Send the event to the bitBucket...
                            if self.bitBucket is not None:
                                if (BitBktIndexationFailures):
                                    self.bitBucket.bitBucket( 3, self.currentRequests[indx][3],
                                                              br.getFailureMessage())

                                    # Accumulate BitBucket stats for indexNames/typeMap.
                                    if statsIdxBb.has_key(idxMapNm):
                                        statsIdxBb[idxMapNm] += 1

                            else:
                                self.logger.warn("No BitBucket for failed BuldRequest action [{}]".format(self.currentRequests[indx][3]))

                if bulkReqRetry is not None:
                    self.logger.info( "Submitted bulkReqRetry object with [{}] items, to Retry Queue.".format(retryActionsCount) )
                    # send the bulkReqRetry object to the retry queue.
                    bulkReqRetry.reqCounter = retryActionsCount
                    bulkReqRetryItem = EsBulkReqRetryItem(bulkReqRetry)
                    bulkReqRetryItem.retryActionsCount = retryActionsCount
                    self.putBulkReqRetryToRetryQueue(bulkReqRetryItem)

                brType = "Initial"
                if retry:
                    brType = "Retry"

                for brFail in statsBulkFail.keys():
                    self.logger.warn("BulkReq [{}] Failures: Count[{}] FailureMsg[{}]".format(brType, statsBulkFail[brFail], brFail))

                for idxn in statsIdx.keys():
                    rt = 0
                    if statsIdxRt.has_key(idxn):
                        rt =  statsIdxRt[idxn]
                    bb = 0
                    if statsIdxBb.has_key(idxn):
                        bb =  statsIdxBb[idxn]
                    self.logger.warn("BulkReq [{}] Failures: Index/typeMap Name[{}] Count[{}] Retry[{}] BitBucket[{}]".format(brType, idxn, statsIdx[idxn], rt, bb))

            # Clear the bulk request after it has been submitted.
            # If not cleared, all requests are resent.
            self.bulkReq = self.esClient.prepareBulk()
            self.currentRequests = ArrayList()
            self.reqCounter = 0

        # Check for items in the bulkReqRetry Queue.
        # - We should hard-fail if the bulkReqRetry Queue gets too large (configable value).
        # - Look at the timing on the bulkReqRetry item.
        #   Once enough time has passed, then retry the bulkReqRetry item.
        # - The bulkReqRetry process can be done here or in it's own thread.
        #   But, first, we will try it out right here.
        # - Keep a running total of the number of bulkReqs and bulkReqItems in the queue.
        #   Report on these stats periodically.
        self.procBulkReqRetries()

    # Process Bulk Request Retries.
    # Bulk Request Retries are placed into a priority queue.
    # This process will check through the priority queue and submit the bulk retry requests
    # after a N sec delay as of thier queue insertion.
    def procBulkReqRetries(self):
        method = "procBulkReqRetries"
        self.logger.debug("[{}.{}] Processing: bulkReqRetryQueue[{}]".format(self.threadName, method, self.bulkReqRetryQueue.qsize()))
        if (not self.bulkReqRetryQueue.empty()):
            # Circulate through bulkRequestRetryQueue.
            # Process requests that need to be processed.
            self.bulkRetryBatchNumber = self.bulkRetryBatchNumber + 1
            batch = self.bulkRetryBatchNumber
            while not self.bulkReqRetryQueue.empty():
                brrObj = self.getQueue()
                if (brrObj.batchNumber >= batch):
                    # If we have a file-list object at this point we'll have circum-processed
                    # all objects in the queue and gone back to the start.
                    # So we'll just reinsert this object back into the queue.
                    # and break out of our loop.
                    self.putQueue(brrObj)
                    self.logger.debug("[{}.{}] Circulated thru bulkReqRetryQueue.".format(self.threadName, method))
                    break 

                self.logger.debug("[{}.{}] Circulating thru bulkReqRet0ryQueue. brrObj.batchNumber[{}] batch[{}]".format(self.threadName,
                                                                                                                         method,
                                                                                                                         brrObj.batchNumber,
                                                                                                                         batch))

                # Has N sec passed since the bulkReqRetry object was added to the queue?
                delayX = int(pytime.time() - brrObj.createdTime)
                self.logger.debug("[{}.{}] brrObj.createdTime[{}] delayX[{}]".format(self.threadName, method, brrObj.createdTime, delayX))
                if delayX >= self.bulkRetryDelaySeconds:
                    # Retry the bulkRequests...
                    self.logger.info("[{}.{}] Submitting a bulkrequest Retry of [{}] items.".format(self.threadName, method, brrObj.bulkReqRetry.reqCounter))
                    brrObj.bulkReqRetry.execute(True)
                else:
                    # Not ready for retry...
                    # Give the fileList object the latest batch number
                    brrObj.batchNumber = batch
                    # Give the fileList object a present-time queue-time
                    brrObj.queTime = pytime.time()
                    # Reinsert into the priority queue.
                    self.putQueue(brrObj)
                    self.logger.debug("[{}.{}] Back into queue: batchNumber[{}] queueTime[{}]".format(self.threadName, method,
                                                                                                      brrObj.batchNumber, brrObj.queTime))
                
    # Used to initially place a fileList object into the FileList Mover Queue.            
    def putBulkReqRetryToRetryQueue(self, bulkReqRetryItem):
        bulkReqRetryItem.queTime = pytime.time()
        self.bulkReqRetryQueue.put((1, bulkReqRetryItem.queTime, bulkReqRetryItem))
        self.logger.info("Added BulkReqRetry Object to BulkReqRetryQueue. IP Queue Size:[{}]".format(self.bulkReqRetryQueue.qsize()))

        jMap = HashMap()
        jMap.put('bulkReqRetryQueueSize', self.bulkReqRetryQueue.qsize())
        if self.esLogger is not None:
           self.esLogger.metrics('EsBulkReq', 'putBulkReqRetryToRetryQueue', jMap)

    # Get a bulkReqRetry item from the queue.
    def getQueue(self):
        method = "getQueue"
        brrObj = None
        try:
            brrObj = self.bulkReqRetryQueue.get()
            self.logger.debug("[{}.{}] Get BulkReqRetry Object from bulkReqRetryQueue.".format(self.__class__.__name__, method))

        except BaseException as ex:
            self.logger.warn("BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            if self.esLogger is not None:
                self.esLogger.warn('EsBulkReq', 'getQueue',
                                   "BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
        except JException as ex:
            self.logger.warn("JException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            if self.esLogger is not None:
                self.esLogger.warn('EsBulkReq', 'getQueue',
                                   "JException in [{}.{}] - [{}]".format(self.threadName, method, ex))

        if brrObj is not None:
            return brrObj[2]
        else:
            return None

    # Place the bulkReqRetry Item back into the queue.
    def putQueue(self, bulkReqRetryItem):
        method = "putQueue"
        try:
            self.bulkReqRetryQueue.put((1, bulkReqRetryItem.queTime, bulkReqRetryItem))
            self.logger.debug("[{}.{}] Put BulkReqRetry Object to FileListIpQueue.".format(self.__class__.__name__, method))

        except BaseException as ex:
            self.logger.error("BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            if self.esLogger is not None:
                self.esLogger.error('EsBulkReq', 'putQueue',
                                    "BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
        except JException as ex:
            self.logger.error("JException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            if self.esLogger is not None:
                self.esLogger.error('EsBulkReq', 'putQueue',
                                    "JException in [{}.{}] - [{}]".format(self.threadName, method, ex))


class TestEsBulkReq(unittest.TestCase):
    def setUp(self):
        self.logger = Logger.getLogger("ElasticSearch.TestEsBulkReq")
        # self.logger.setLevel(Level.DEBUG)

        self.logger.debug("running test: TestEsBulkReq")

        # Pull date from out test properties file...
        esTestProps    = Properties('./lib/ElasticSearchTest.properties')
        esTestCluster  = esTestProps.getProperty('esTest.cluster')
        esTestHosts    = esTestProps.getProperty('esTest.hosts')
        esTestNodeName = esTestProps.getProperty('esTest.node.name')
        esTestIndex    = esTestProps.getProperty('esTest.index')

        # Create the EsNode instance.
        self.esNc = EsNode(esTestCluster, esTestHosts, esTestNodeName)

        # 1st create the EsLoggerConfig object with
        # indexName and a new EsLoggerQueue.
        esLoggerConfig = EsLoggerConfig(self.esNc, esTestHosts, esTestIndex, EsLoggerQueue(1000))
        esLoggerConfig.useBulkReq = False

        # Create the EsLogger instance
        self.esLogger = EsLogger(None, esLoggerConfig)

    def testEsBulkReq(self):
        bulkReq = EsBulkReq(self.esNc.getClient(),
                            self.esLogger,
                            90)
        self.assert_(bulkReq is not None)

        # Close the ES Logger.
        self.esLogger.close()

        # Close the ES Node.
        self.esNc.close()



''' =================================================================
    EsLogger (Send logs directly into ES)
    =================================================================
'''

''' =================================================================
    This is the queue for the EsLogger.
    The 1st EsLogger instantiation is essentially the WorkerController.
    The 1st EsLogger instance creates another (worker) instance that
    runs on a thread and pulls log events off of the worker queue and
    logs them to the ES index.
    We use a reference to that first instance to place log objects
    into the WokerQueue.
'''
class EsLoggerQueue(WorkerQueue):
    def __init__(self, queueSize):
        WorkerQueue.__init__(self, queueSize)


''' =================================================================
    This is the EsLogger work item.
    Work items are log event objects that are placed into the worker queue.
    The log event objects are pulled off of the queue by the worker instance
    and logged to the ES index.
'''
class EsLoggerWorkItem(WorkItem):
    def __init__(self, mod, cls, mthd, jMap):
        self.module = mod
        self.class = cls
        self.method = mthd
        self.jMap = jMap


''' =================================================================
    Various configuration values are placed into the EsLoggerConfig
    object to configure the threaded instance.
    The main thing that it will hold are the name of the default index to
    push log events into, and the worker queue to pull log events from.
'''
class EsLoggerConfig(WorkerConfiguration):
    def __init__(self, esNode, esHosts, indexName, workerQueue):
        self.esNode = esNode
        self.esHosts = esHosts
        self.indexName = indexName
        self.wq = workerQueue
        self.useBulkReq = None
        # We won't bother with a message size trigger for the esLogger for now.
        # Execute if number of actions == 1000
        self.bulkReqExecCountTrigger = 1000


''' =================================================================
    Class for logging data directly into an ES index.
    With this class, we have the option to use bulk-requests for more
    efficient log insertion.

    The EsLogger runs as a thread.
    Multiple classes and methods, possibily running on their own threads
    need to output data to an ES Index. Bringing up an EsIndexer is expensive
    as each one is a Node/Client in the ES Cluster.
    So rather than creating a new Node/Client for each logging object,
    the logging object should only place a log request into a queue,
    and the EsLoggerThread reads that queue and logs events to the
    the appropriate index.

    This class uses the WorkerThread model, but the main difference
    is that the first instance of the object becomes the controller of
    a second instance that it instantiates and threads off.
    The objects that need to log data directly to ES use a handle
    to the first instance (the controller) to place log events into
    the worker queue using the logToEs() method. The second instance
    (running on a thread), the actuall worker, pulls log events off of
    the work queue and pushes that log message into ES.
'''
class EsLogger(WorkerThread):
    def __init__(self, threadName, workerConfig):
        WorkerThread.__init__(self, threadName, workerConfig)
        
        self.logger = Logger.getLogger("ElasticSearch.EsLogger")
        # self.logger.setLevel(Level.DEBUG)

        self.workerConfig = workerConfig

        # Default Bulk Request Settings for ES Logging.
        # - Set to True to use bulk requests for logs.
        self.useBulkReq = False
        self.bulkReqCounter = 0
        self.bulkReqExecCountTrigger = 1000
        self.lastBulkReqFlush = datetime.now()

        if (threadName is None):
            # ==== 1st Instance (threadName is None) ====
            # Get the EsLogger queue.
            # This object will feed the queue through this reference.
            self.wq = workerConfig.wq
            self.esNode = self.workerConfig.esNode

        else:
            # ==== 2nd Instance (threadName is not None) ====
            self.esNode = self.workerConfig.esNode
            self.esClient = self.esNode.getClient()
            self.esBulkReq = EsBulkReq(self.esClient, None)

            self.indexName = workerConfig.indexName
            
            # If bulkReq config are set in the workerConfig object, use them.
            if workerConfig.useBulkReq is not None:
                self.useBulkReq = workerConfig.useBulkReq
            if workerConfig.bulkReqExecCountTrigger is not None:
                self.bulkReqExecCountTrigger = workerConfig.bulkReqExecCountTrigger

        # Json SerDe objects
        self.boon = BoonJson()

        self.esLoggerWorker = None
        self.esLoggerThread = None
        self.stopThread = False
        self.threaded = False

        self.dtfmt = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

        if threadName is None:
            self.threadName = "EsLoggerController"
            # Startup the Background thread.
            self.startEsLoggerThread()
        else:
            self.threadName = threadName

    def startEsLoggerThread(self):
 
        if (     (self.esLoggerWorker is None)
             and (self.esLoggerThread is None) ):
            # Create the workers config object.
            # Place the esLogger worker queue into the config object.
            # Place the index name into the config object.
            wconfig = EsLoggerConfig(self.esNode, self.workerConfig.esHosts,
                                     self.workerConfig.indexName, self.wq)
            
            wconfig.useBulkReq = self.workerConfig.useBulkReq
            wconfig.bulkReqExecCountTrigger = self.workerConfig.bulkReqExecCountTrigger

            if self.workerConfig.esNode is not None:
                esNode = self.workerConfig.esNode
            else:
                esNode = self.esNode

            if esNode == 0:
                self.logger.error("An esNode is required to create the EsLogger WorkerThread.")
                sys.exit(0)

            wconfig.esNode = esNode
            wconfig.esHosts = self.workerConfig.esHosts

            # Create the esLogger object that will run on a thread.
            # Give the object a thread name and the workerConfig object.
            # This esLogger object will run on a thread and feed off of the
            # worker queue in the worker config object.
            self.esLoggerWorker = EsLogger("EsLoggerThread", wconfig)
            # Create the thread.
            self.esLoggerThread = Thread(name=self.esLoggerWorker.threadName,
                                         target=self.esLoggerWorker.start, args=())
            # Start the thread.
            self.esLoggerThread.start()
        else:
            self.logger.warn("The EsLoggerThread is already running!")

    
    def stopEsLoggerThread(self):
        if self.esLoggerWorker.useBulkReq:
            self.logger.debug("Flush Bulk Inserts...")
            self.esLoggerWorker.flushBulkInserts()

        # Give 7 seconds to flush any pending inserts.
        self.logger.debug("-- Sleep 2...")
        pytime.sleep(7)

        if self.esLoggerWorker:
            self.logger.debug("Stop Thread...")
            self.esLoggerWorker.stopThread = True

            # Allow 2 seconds for thread to die
            self.logger.debug("-- Sleep 2...")
            pytime.sleep(2)

            # Use interrupts to stop threads.
            # Interrupt is needed because the thread may be waiting on queue queue.
            self.logger.debug("Interrupt Thread...")
            tries = 5
            while self.esLoggerWorker.isAlive() and tries > 0:
                self.esLoggerWorker.interrupt()
                pytime.sleep(.002)
                tries = tries - 1


    def stop(self):
        self.stopEsLoggerThread()

    def run(self):
        self.threaded = True
        self.wq = self.workerConfig.wq

        while not self.stopThread:
            # Read from workerQueue.
            wrk = self.dequeueWork()

            if wrk is not None:
                # Log to ES
                logLevel = wrk.get('logLevel')
                self._logToEs(logLevel, wrk)
            else:
                # Flush bulk inserts if there are any in the que,
                # Every 5 seconds.
                unFlushedSecs = (datetime.now() - self.lastBulkReqFlush).seconds
                if (    (unFlushedSecs >= 5)
                    and (self.bulkReqCounter > 0)):
                    self.logger.debug("...EsLogs Bulk Inserts... Flushing after 5 seconds...")
                    self.flushBulkInserts()

    # Place work into the work queue.
    def enqueueWork(self, workObj):
        method = "enqueueWork"
        self.logger.debug("[{}.{}] Place workObj into workerQueue".format(self.threadName,
                                                                          method) )
        self.wq.put(workObj)


    # Pulls a work item from the queue.
    def dequeueWork(self):
        method = "dequeueWork"
        # True: blocks if there are no items in the queue.
        #    3: blocking on the queue releases after 1 seconds.
        try:
            wrk = None
            wrk = self.wq.get(True,1)
        except BaseException as ex:
            # When we hit a BaseException while waiting on workQue, it is prob a timeout error on and empty queue.
            if ('Empty' not in type(ex).__name__):
                self.logger.error("BaseException in [{}.{}] - [{}: {}]".format(self.threadName, method, type(ex), ex))
            if wrk is None:
                self.logger.debug("--> Getting wrk, Queue.get() - timed out.")
        except JException as ex:
            self.logger.info("JException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            # If we get an interrupt which waiting for work, we need to stop the thread.
            self.stopThread = True
        return wrk


    def logToEs(self, logLevel, jMap):

        jMap.put('logDateTime', self.dtfmt.format(datetime.now()))
        jMap.put('logLevel', logLevel)

        # Push data into queue and return
        self.enqueueWork(jMap)

        
    # Accepts a Java Map object which we can serialize a json string via Boon.
    # Then use the json string to store to ES.
    def _logToEs(self, logLevel, jMap):
        # *** Interactive Debug seems to mess with multi-threading
        # pdb.set_trace()

        # Convert the Java Map to a JSON string
        jsonEvent = self.boon.serToJson(jMap)

        try:
            # Get the date from the logDateTime
            edt = jMap.get('logDateTime')
            # Pull the data from the TimeStamp 
            pat = r'^(\d+)\-(\d+)\-(\d+)T.*' 
            match = re.search(pat, edt) 
            if match: 
                edt = match.group(1) +  match.group(2) +  match.group(3) 
            else:
                edt = 'unknown'

            # Add the date to the index name.
            indexName = self.indexName + '-' + edt

            # Prep to Index document.
            self.logger.debug("\n==index==> index[{}] logLevel[{}]\n".format(indexName,logLevel))
            i1 = self.esClient.prepareIndex(indexName, logLevel, None)
            i2 = i1.setSource(jsonEvent)

            self.doEsInsert(i2, indexName, '', jsonEvent)

        except JException as ex:
            # ex: g.elasticsearch.index.mapper.MapperParsingException:...
            #     failed to parse [Data.Return.Value.Death.DateNormalized]
            # Change the name of the bad field to s_<fieldName> in the jsonStr and reinsert the object.
            # - Call on self.indexEvent(newJson) recursively as there may be multiple failures.
            self.logger.error("EsLogger Exception Caught [{}] [{}]".format(ex, jsonEvent))

    def doEsInsert(self, i2, indexName, typeMap, jsonEvent):
        #=================================================================================
        # Single index request operation...
        # Submits one index operation at a time.
        #=================================================================================
        if not self.useBulkReq:
            self.logger.debug("\n==single==> {}\n".format(jsonEvent))
            # Execution a single index request.
            indxResp = i2.execute().actionGet()
        #=================================================================================

        #=================================================================================
        # Bulk Index Operation...
        # Keep adding to the bulk index request until we reach N index operations,
        # then submit the request.
        #=================================================================================
        else:
            self.logger.debug("\n==bulk==> {}\n".format(jsonEvent))
            self.esBulkReq.add(i2, indexName, typeMap, jsonEvent)
            self.bulkReqCounter = self.bulkReqCounter + 1
            if self.bulkReqCounter >= self.bulkReqExecCountTrigger:
                self.esBulkReq.execute()
                self.bulkReqCounter = 0
                self.lastBulkReqFlush = datetime.now()
        #=================================================================================

    # If Bulk Request mode is in use, one should call on this function to flush
    # any pending bulk requests at the end of bulk operation cycles.
    def flushBulkInserts(self):
        if self.useBulkReq and (self.bulkReqCounter > 0):
            self.logger.debug("\n==> BulkReq Flush!\n")
            self.esBulkReq.execute()
            self.lastBulkReqFlush = datetime.now()


    def close(self):
        self.stopEsLoggerThread()


''' It is best to EsLogger tests on an EsNode, as the Master EsNode may not be found.
'''
class TestEsLogger(unittest.TestCase):
    def setUp(self):
        self.logger = Logger.getLogger("ElasticSearch.TestEsLogger")
        # self.logger.setLevel(Level.DEBUG)

        self.logger.debug("running test: TestEsLogger")

    def testLoggerSingle(self):

        # -------------------------------------------
        # Single Insert Test
        # -------------------------------------------

        # Pull date from out test properties file...
        esTestProps    = Properties('./lib/ElasticSearchTest.properties')
        esTestCluster  = esTestProps.getProperty('esTest.cluster')
        esTestHosts    = esTestProps.getProperty('esTest.hosts')
        esTestNodeName = esTestProps.getProperty('esTest.node.name')
        esTestIndex    = esTestProps.getProperty('esTest.index')

        # Create the EsNode instance.
        esNode = EsNode(esTestCluster, esTestHosts, esTestNodeName)

        # 1st create the EsLoggerConfig object with
        # indexName and a new EsLoggerQueue.
        esLoggerConfig = EsLoggerConfig(esNode, esTestHosts, esTestIndex, EsLoggerQueue(1000))
        esLoggerConfig.useBulkReq = False

        # Create the EsLogger instance
        esLogger = EsLogger(None, esLoggerConfig)

        # Generate our logging Java Map 
        # (which will be converted to a JSON string)
        # for indexing to ES.
        jLog = HashMap()
        jLog.put('desc', "This my description... Which is mine! Ahgemmm!")
        obj1 = HashMap()
        obj1.put('fld1', "abcdefg")
        obj1.put('fld2', "1234567890")
        obj1.put('fld3', 1234567890)
        jLog.put('obj1', obj1)

        # Now, we log to the ES Index "01_test"
        esLogger.logToEs('INFO', jLog)
        esLogger.logToEs('INFO', jLog)
        esLogger.logToEs('INFO', jLog)
        esLogger.logToEs('INFO', jLog)
        esLogger.logToEs('INFO', jLog)
        esLogger.logToEs('INFO', jLog)

        # Finally, close the esLogger object.
        esLogger.close()

        self.logger.debug("Single Insert Test completed...")

        pytime.sleep(5)

        # -------------------------------------------
        # Bulk Insert Test
        # -------------------------------------------
        # 1st create the EsLoggerConfig object with
        # indexName and a new EsLoggerQueue.
        esLoggerConfig = EsLoggerConfig(esNode, esTestHosts, "01_test", EsLoggerQueue(1000))
        esLoggerConfig.useBulkReq = True
        esLogger = EsLogger(None, esLoggerConfig)

        # Generate our logging Java Map 
        # (which will be converted to a JSON string)
        # for indexing to ES.
        jLog = HashMap()
        jLog.put('desc', "This my description... Which is mine! Ahgemmm!")
        obj1 = HashMap()
        obj1.put('fld1', "abcdefg")
        obj1.put('fld2', "1234567890")
        obj1.put('fld3', 1234567890)
        jLog.put('obj1', obj1)

        # Now, we log to the ES Index "01_test"
        esLogger.logToEs('TRACE', jLog)
        esLogger.logToEs('TRACE', jLog)
        esLogger.logToEs('TRACE', jLog)
        esLogger.logToEs('TRACE', jLog)
        esLogger.logToEs('TRACE', jLog)
        esLogger.logToEs('TRACE', jLog)

        self.logger.debug("Bulk Test Sleeping for 15 seconds...")
        pytime.sleep(7)

        # Finally, close the esLogger object.
        esLogger.close()

        self.logger.debug("Bulk insert test competed.")

        self.assert_(esLogger.wq.qsize() == 0)

        # Read the index and find out test log event.
        None

        # Drop the "01_test" index
        None

        # Finally, close the EsNode...
        esNode.close()


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
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestEsNode)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestEsLogger)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestEsBulkReq)
        # unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestHostBuckets)
        # unittest.TextTestRunner(verbosity=2).run(suite)

    else: 
        if (sys.argv[1] != 'integration'):
            # ------------------------------------------------
            # How to run tests that have a different prefix...
            # ------------------------------------------------
            runner = unittest.TextTestRunner()
            mySuite = unittest.makeSuite(TestHdfsUtil, prefix='integration')
            runner.run(mySuite)
