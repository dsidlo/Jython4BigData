#!/usr/local/bin/jython27 --hadoop --
'''
The WorkerThreads module is a set of classes that represent a useful pattern for dividing work
up and having that work done by a set of WorkerThreads (Casual Worker Design Pattern).

The Classes...

- WorkerThreadController
- WorkerConfiguration
- WorkerThread
- WorkerQueue
- WorkItem

WorkerThreadController...
The WorkerThreadController creates a WorkerConfiguration object and sends it to a
newly created WorkerThread.
Multiple WorkerThreads are created (w/appropriate WorkerConfigurations).
Multiple WorkerThreads are started.
The WorkerThreadController looks for work to do.
When is has some work that needs to be done, it places the work (as a WorkItem) into
the WorkerQueue for WorkerThreads to pickup and process.
The WorkerThreadController Watches the state of WorkerThreads, to make sure that they
continue to be processing data in an orderly fashion. Watching that WorkerThreads
are not getting "stuck", and taking appropriate action to unstick or restart
WorkerThreads as needed.
The WorkerThreadController is also responsible for shutting down WorkerThreads.

WorkerThread...
A WorkerThread receives a WorkerConfiguration on instantiation.
Once the WorkerThread is started, it waits on the WorkerQueue for some work to do.
If it sees work on the WorkerQueue it attempts to pull some work off of the queue.
If it successfully pulls a WorkItem off of the WorkerQueue, it performs the work.
Once it complete the work, it once again wait for work on the WorkerQueue.
The WorkerThread may be interrupted by the WorkerThreadController, and stopped,
at which point, the WorkerThread will stop waiting on the WorkerQueue, and exit.
The WorkerThread gathers stats regarding the past work that it had done, and the
current work in progress. It may report on these stats, or it may rely on the
WorkerThreadController to collect and report these stats.

WorkerConfiguration....
The WorkerConfiguration object is a simple object that contains properties that
are passed into a given WorkerThread.

WorkerQueue...
The WorkerQueue is an object that is based on the Jython Queue.
WorkItemects are placed into the WorkerQueue by the WorkerThreadController.
WorkerThreads pull work to be perfomed, off of the WorkerQueue.

WorkItem...
The WorkItem is a simple object that contains properties and possibly methods
regarding the work that needs to be performed. 

*** Tips...
When creating and using the WorkerThread classes, please keep in mind that we want to
minimize object sharing between threads. The only object that should be shared
across threads is the WorkerQueue. WorkItems placed on the WorkerQueue should be
single instances that are not shared with other WorkerThreads or the main threads.
Keep these points in mind, and you don't have to deal with the complexity of
locks, condition,s events, and/or semaphore objects.

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

from java.lang         import Exception as JException
from java.lang         import Thread as JThread, InterruptedException

from org.apache.log4j  import Logger, Level

from threading import Thread, Lock, RLock, Condition, Semaphore, BoundedSemaphore, Event, Timer
from random    import randrange
from Queue     import *

import unittest, inspect, md5, string, socket, datetime, re, sys, time

''' ============================================================================
    This is a configuration object for WorkerThreads.
    Inherit this class to create a configuration object for your own 
    WorkerThreads.
'''
class WorkerConfiguration():
    def __init__(self):
        # Perform 20 loops.
        self.loops  = 20
        self.cv     = Condition()
        self.cvWait = 20


''' ============================================================================
    Override this object if you have special requirements for you queue.
'''
class WorkerQueue(Queue):
    def __init__(self, queueSize):
        Queue.__init__(self, queueSize)


''' ============================================================================
    Inherit this object to creat your workitems.
    Normaly, workitems should only contain a set of properties, but may contain
    methods that the worker can call on if needed.
'''
class WorkItem():
    def __init__(self):
        None


''' ============================================================================
    Inherit this class for your own WorkerThread.
    Override the methods appropriately.
'''
class WorkerThread(JThread):
    def __init__(self, tName, workerConfig):
        JThread.__init__(self)
        self.config = workerConfig


    def run(self):
        self.stopThread = False
        self.thread = self.currentThread()


    def stop(self):
        self.stopThread = True


    def dequeueWork(self):
        None



''' ============================================================================
    Inherit this class for your own WorkerThreadController.
    Override the methods appropriately.
'''
class WorkerThreadController():
    def __init__(self):
        None

    def createWorkers(self, workerClass, numWorkers):
        None

    def startWorkers(self):
        None

    def stopWorkers(self):
        None

    def queueWork(self, workObj):
        None



''' ============================================================================
    ============================================================================
    Test Classes used to test the Classes above...
    ============================================================================
    ============================================================================
'''

''' ============================================================================
    Simple Treading test, self managed threads started and stopped.
    - Stopped using a flag variable.
    - Stopped using java thread interrupt.
    - These WorkerThreads are self-managed, in that they are simple loops that
      would eventually exit. The WorkerThreads don't comsume WorkItems from a
      WorkQueue.
    - We simply want to spin threads up and stop them via a flag or via an
      interrupt.
    ============================================================================
'''

class MyWorkerConfig(WorkerConfiguration):
    def __init__(self):
        self.loops   = 10
        self.cvTest  = True
        self.cv      = Condition()
        self.cvWait  = 20


class MyWorker(WorkerThread):
    def __init__(self, tName, workerConfig):
        JThread.__init__(self)

        self.logger = Logger.getLogger("MyWorker")
        # self.logger.setLevel(Level.DEBUG)

        self.threadName        = tName
        self.thread         = None
        self.stopProc       = False
        self.wasStopped     = False
        self.wasInterrupted = False

        self.config = workerConfig
        self.loops  = self.config.loops
        self.cvTest = self.config.cvTest
        self.cv     = self.config.cv
        self.cvWait = self.config.cvWait

        self.logger.debug("Looping Worker Created: {}".format(tName))


    def run(self):
        self.stopProc = False
        self.thread   = self.currentThread()
        self.loopTest()


    def stop(self):
        self.stopProc = True


    def loopTest(self):
        with self.cv:
            try:
                self.logger.debug("{}: loopTest() waiting...".format(self.threadName))
                # Without a timeout value on wait, interrupt on wait only prints "Interrupted thread" (internally),
                # but does not seem to break the waiting block on the conditional. And, none
                # of the except's below are hit.
                # With a value, wait will sleep, and interrupt with break through wait and hit KeyboardInterrupt.
                if self.cvTest is True:
                    self.cv.wait(self.cvWait)

                for i in xrange(self.loops):
                    self.logger.debug("{}: loopTest({} of {})".format(self.threadName, i + 1, self.loops))
                    time.sleep(1)
                    if self.stopProc:
                        self.wasStopped = True
                        self.logger.debug("{}: Stopped!".format(self.threadName))
                        break

            except InterruptedException as ex:
                # This is the exception that is hit when interrupt is called on this thread.
                self.logger.warn("{} - Got InterruptedException: {}.".format(self.threadName, str(ex)))
                self.wasInterrupted = True

            except BaseException as ex:
                self.logger.warn("{} - Got BaseException: {}.".format(self.threadName, str(ex)))

            except Exception as ex:
                self.logger.warn("{} - Got Exception: {}.".format(self.threadName, str(ex)))

            except JException as ex:
                self.logger.warn("{} - Got JException: {}.".format(self.threadName, str(ex)))

            except KeyboardInterrupt as ex:
                # Interrupt on time.sleep() is caught here...
                # Also catches wait(<timeout>) with timeout specified.
                self.logger.info("{} - Got KeyboardInterrupt: {}.".format(self.threadName, str(ex)))

            finally:
                self.logger.debug("{} finally!".format(self.threadName))
                

        self.logger.debug("{} Finished.".format(self.threadName))


class MyWorkerController(WorkerThreadController):
    def __init__(self):
        self.logger = Logger.getLogger("MyWorkerController")
        # self.logger.setLevel(Level.DEBUG)

    def createWorkers(self, workerClass, numWorkers, createCond):
        self.threads = []
        self.loopers = []
        self.condValue = Condition()

        for i in xrange(numWorkers):
            wconfig = MyWorkerConfig()
            if createCond:
                wconfig.cvTest = True
            else:
                wconfig.cvTest = False
            l = workerClass("thread-{}".format(i), wconfig)
            self.loopers.append(l)
            self.threads.append(Thread(name=l.threadName, target=l.start, args=()))


    def startWorkers(self):
        self.logger.debug("=====> Starting...")
        for thread in self.threads:
            self.logger.debug("Starting Thread %s" % thread.name)
            thread.start()


    def stopWorkersViaStop(self):
        self.logger.debug("=====> Canceling...")
        for l in self.loopers:
            l.stop()

        for thread in self.threads:
            thread.join()


    def stopWorkersViaInterrupt(self):
        self.logger.debug("=====> Interrupts...")
        for l in self.loopers:
            l.thread.interrupt()

        for thread in self.threads:
            thread.join()


    def queueWork(self, workObj):
        None

''' This class tests the self-manage thread classes above.
    The self-managed threads test the WorkerThread and WorkerThreadController
    threading model classes.
'''

class TestWorkerThreadController(unittest.TestCase):
    def testWorkers_Stop(self):
        self.logger = Logger.getLogger("TestWorkerThreadController")
        # self.logger.setLevel(Level.DEBUG)

        wc = MyWorkerController()
        # False: Don't create Conditional, test look flag (stopProc).
        wc.createWorkers(MyWorker, 50, False)
        wc.startWorkers()
        # Check running state of threads.
        # Allow threads to startup.
        time.sleep(0.05)
        for t in wc.loopers:
            self.assert_(t.isAlive() == True)
        # Check initial state of validation variables.
        for t in wc.loopers:
            self.assert_(t.stopProc == False)
            self.assert_(t.wasStopped == False)
            self.assert_(t.wasInterrupted == False)
        self.logger.debug("=====> Sleeping...")
        time.sleep(3)
        wc.stopWorkersViaStop()
        # Give threads some time to quiesce.
        time.sleep(1)
        # Check that all threads were stopped via sopProc flag.
        for t in wc.loopers:
            self.assert_(t.isAlive() == False)
        # Check that all threads are not in a running state.
        for t in wc.loopers:
            self.assert_(t.stopProc == True)
            self.assert_(t.wasStopped == True)
            self.assert_(t.wasInterrupted == False)

    def testWorkers_Interrupt(self):
        self.logger = Logger.getLogger("TestWorkerThreadController")
        # self.logger.setLevel(Level.DEBUG)

        wc = MyWorkerController()
        # True: Create Conditional to test Interrupt.
        wc.createWorkers(MyWorker, 50, True)
        wc.startWorkers()
        # Check running state of threads.
        for t in wc.loopers:
            self.assert_(t.isAlive() == True)
        # Check initial state of validation variables.
        for t in wc.loopers:
            self.assert_(t.stopProc == False)
            self.assert_(t.wasStopped == False)
            self.assert_(t.wasInterrupted == False)
        self.logger.debug("=====> Sleeping...")
        time.sleep(3)
        wc.stopWorkersViaInterrupt()
        # Give threads some time to quiesce.
        time.sleep(1)
        # Check that all threads were stopped via sopProc flag.
        for t in wc.loopers:
            self.assert_(t.isAlive() == False)
        # Check that all threads are not in a running state.
        for t in wc.loopers:
            self.assert_(t.stopProc == False)
            self.assert_(t.wasStopped == False)
            self.assert_(t.wasInterrupted == True)



''' ============================================================================
    WorkerThread model test.
    - WorkItems are prepared, and fed into the WorkerQueue.
    - WorkersThreads pull work from the WorkerQueue and perform the required
      WorkActivities.
    ============================================================================
'''

# WorkItem 1 of 2
class wtShootLasers(WorkItem):
    def __init__(self):
        self.logger = Logger.getLogger("wtShootLasers")
        # self.logger.setLevel(Level.DEBUG)

        colors = [ "Red", "Gree", "Blue", "Pink", "Magenta" ]
        self.color = colors[randrange(len(colors))]
        self.repeat = randrange(4)+1

    def workAction(self):
        for n in xrange(self.repeat):
            self.logger.debug("Shoot {} Laser.".format(self.color))

# WorkItem 2 of 2
class wtThrowRocks(WorkItem):
    def __init__(self):
        self.logger = Logger.getLogger("wtThrowRocks")
        # self.logger.setLevel(Level.DEBUG)

        rocks = [ "Grain of Sand", "Gravel", "Pebble", "Stone", "Bolder" ]
        self.rock = rocks[randrange(len(rocks))]
        self.repeat = randrange(4)+1

    def workAction(self):
        for n in xrange(self.repeat):
            self.logger.debug("Throw {}.".format(self.rock))


class wtWorkQueue(WorkerQueue):
    def __init__(self, queueSize):
        # We will use a normal queue which will block on insertion if it already
        # contains 1000 items.
        WorkerQueue.__init__(self, queueSize)


class wtGundamWarriorConfig(WorkerConfiguration):
    def __init__(self):
        self.workerQueue = None


class wtGundamWarrior(WorkerThread):
    def __init__(self, tName, workerConfig):
        JThread.__init__(self)

        self.logger = Logger.getLogger("wtGundamWarrior")
        # self.logger.setLevel(Level.DEBUG)

        self.config = workerConfig
        self.wq = self.config.workerQueue
        self.threadName = tName
        self.stopThread = False
        self.wrkItmsProcessed = 0

    def run(self):
        self.stopThread = False
        self.thread = self.currentThread()
        method = "run"
        wrkItm = None
        try:
            wrkItm = self.dequeueWork(self.wq)
            self.logger.debug("Pulled wrkItm[{}] from queue".format(wrkItm.__class__.__name__))
            while (self.stopThread == False):
                if (wrkItm is not None):
                    self.logger.debug("Processing wrkItm...")
                    wrkItm.workAction()
                    self.wrkItmsProcessed = self.wrkItmsProcessed + 1
                wrkItm = self.dequeueWork(self.wq)
                self.logger.debug("Pulled wrkItm[{}] from queue".format(wrkItm.__class__.__name__))
        except BaseException as ex:
            self.logger.warn("BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
        except JException as ex:
            self.logger.info("JException in [{}.{}] - [{}]".format(self.threadName, method, ex))

    def stop(self):
        self.stopThread = True

    # Pulls a work item from the queue.
    def dequeueWork(self, workQueue):
        method = "dequeueWork"
        # True: blocks if there are no items in the queue.
        #    3: blocking on the queue releases after 3 seconds.
        try:
            wrk = None
            wrk = workQueue.get(True,3)
        except BaseException as ex:
            # When we hit a BaseException while waiting on workQue, it is prob a timeout error on the queue.
            if (ex != ""):
                self.logger.warn("BaseException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            if wrk is None:
                self.logger.debug("--> Getting wrk, Queue.get() - timed out.")
        except JException as ex:
            self.logger.info("JException in [{}.{}] - [{}]".format(self.threadName, method, ex))
            # If we get an interrupt which waiting for work, we need to stop the thread.
            self.stopThread = True
        return wrk


class wtGundamWarriorController(WorkerThreadController):
    def __init__(self, queueSize):
        self.logger = Logger.getLogger("wtGundamWarriorController")
        # self.logger.setLevel(Level.DEBUG)

        self.queueSize = queueSize
        self.wq = wtWorkQueue(queueSize)
        self.workItemsCreated = 0;

    def createWorkers(self, numWarriors):
        self.threads = []
        self.warriors = []

        for i in xrange(numWarriors):
            wconfig = wtGundamWarriorConfig()
            wconfig.workerQueue = self.wq
            w = wtGundamWarrior("thread-{}".format(i), wconfig)
            self.warriors.append(w)
            self.threads.append(Thread(name=w.threadName, target=w.start, args=()))

    def startWorkers(self):
        self.logger.debug("=====> Starting Warriors...")
        for thread in self.threads:
            self.logger.debug("Starting Thread %s" % thread.name)
            thread.start()

    def controllTheWorkers(self, workTime):
        self.logger.debug("=====> Controlling Warriors...")
        stm = datetime.datetime.now()
        etm = datetime.datetime.now()
        attackTypes = [wtShootLasers, wtThrowRocks]
        workSecs = (etm - stm).seconds
        # self.logger.debug("workSecs[{}]...".format(workSecs)
        while workSecs < workTime:
            wi = attackTypes[randrange(len(attackTypes))]()
            self.logger.debug("Enqueueing WorkItem [{}]".format(wi.__class__.__name__))
            self.enqueueWork(wi)
            self.workItemsCreated = self.workItemsCreated + 1
            time.sleep(0.05)
            etm = datetime.datetime.now()
            workSecs = (etm - stm).seconds 
            # self.logger.debug("workSecs[{}]...".format(workSecs)

    def stopOrInterruptWarriors(self):
        self.logger.debug("=====> Stopping Warriors...")
        for w in self.warriors:
            x = randrange(2)
            if x == 0:
                self.logger.debug("---> Stopping: [{}]".format(w.threadName))
                w.stop();
            else:
                self.logger.debug("---> Interrupting: [{}]".format(w.threadName))
                # Execute a few interrupts on the thread if necessary.
                tries = 5
                while w.isAlive() and tries > 0:
                    w.interrupt()
                    time.sleep(.002)
                    tries = tries - 1
                
    def stopWorkers(self):
        self.logger.debug("=====> Canceling...")
        for w in self.warriors:
            w.stop()

        for thread in self.threads:
            thread.join()

    def interruptWorkers(self):
        self.logger.debug("=====> Interrupts...")
        for w in self.warriors:
            w.thread.interrupt()

        for thread in self.threads:
            thread.join()

    def enqueueWork(self, workObj):
        self.wq.put(workObj)


''' This class tests the self-manage thread classes above.
    The self-managed threads test the WorkerThread and WorkerThreadController
    threading model classes.
'''

class TestwtGundamWarriorController(unittest.TestCase):
    def _testWarriors(self, queSize, numWarriors, feedSecs):
        self.logger = Logger.getLogger("TestwtGundamWarriorController")
        # self.logger.setLevel(Level.DEBUG)

        # - Queue size.
        gwCtrl = wtGundamWarriorController(queSize)
        # - Number of workers.
        gwCtrl.createWorkers(numWarriors)

        # Check running state of threads.
        for t in gwCtrl.warriors:
            self.assert_(t.isAlive() == False)
        # Check initial state of validation variables.
        for t in gwCtrl.warriors:
            self.assert_(t.stopThread == False)

        # Verify that the worker queue is empty.
        self.assert_(gwCtrl.wq.empty())

        # Start the worker threads.
        gwCtrl.startWorkers()

        # Wait a bit to ensure that the workers startup.
        time.sleep(0.05)

        # Check running state of threads.
        for t in gwCtrl.warriors:
            self.assert_(t.isAlive() == True)
        # Check initial state of validation variables.
        for t in gwCtrl.warriors:
            self.assert_(t.stopThread == False)

        # Have the Workercontroler start pushing work into the
        # WorkerQueue.
        # - Run for N seconds...
        gwCtrl.controllTheWorkers(feedSecs)

        # Sleep and let the work queue empty.
        time.sleep(3)

        gwCtrl.stopOrInterruptWarriors()
        # Setting the stopthread flag won't immediately stop threads that
        # are blocking for a workItem, but will eventually stop if the
        # Queue.get is blocking with a timeout.
        gwCtrl.interruptWorkers()

        # Allow threads to quiesce.
        time.sleep(0.05)

        # Check running state of threads.
        for t in gwCtrl.warriors:
            self.assert_(t.isAlive() == False)
        # Check initial state of validation variables.
        for t in gwCtrl.warriors:
            self.assert_(t.stopThread == True)

        # Verify that the worker queue is empty.
        self.assert_(gwCtrl.wq.empty())

        # Check on the number of work items generated.
        wiCreated = gwCtrl.workItemsCreated
        # Count the number of work items processed by workers, which should equal
        # the number of work items generated.
        wiProcessed = 0
        for t in gwCtrl.warriors:
            wiProcessed = wiProcessed + t.wrkItmsProcessed
        self.logger.debug("WorkItems - Created:[{}] Processed:[{}]".format(wiCreated, wiProcessed))
        self.assert_(wiCreated == wiProcessed)


    def testWarriors(self):
       self._testWarriors(10,  1, 10)
       self._testWarriors(25, 20, 30)
       self._testWarriors(50, 50,  5)
       self._testWarriors(50, 50, 60)

    def shortTest(self):
       self._testWarriors(10,  1, 10)


''' ============================================================================
'''
if __name__ == '__main__':

    if (len(sys.argv) == 1):
        # ----------------
        # Run all tests...
        # ----------------
        # unittest.main()
    
        # ---------------------
        # Run specific tests...
        # ---------------------
        suite = unittest.TestLoader().loadTestsFromTestCase(TestwtGundamWarriorController)
        unittest.TextTestRunner(verbosity=2).run(suite)
        # ---
        # suite = unittest.TestLoader().loadTestsFromTestCase(TestHdfsUtil)
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
            mySuite = unittest.makeSuite(TestwtGundamWarriorController, prefix='integration')
            runner.run(mySuite)
        elif (sys.argv[1] != 'short'):
            # ------------------------------------------------
            # How to run tests that have a different prefix...
            # ------------------------------------------------
            runner = unittest.TextTestRunner()
            mySuite = unittest.makeSuite(TestwtGundamWarriorController, prefix='short')
            runner.run(mySuite)
