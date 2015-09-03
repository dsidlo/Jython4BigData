#!/usr/local/bin/jython27 --

''' Usage...

Check out the tests.


    Tips:

Boon and return unexpected types...
- After deserializing from a JSON string to a JMap, a date component
  that would usually be a string, in some cases turns out to be a
  java.util.Date. I would process the date string via a regexp to
  pull to 'YYYY-MM-DD' components, which works if the value returned
  for my field was a string, but, the regexp would throw an exception
  when the value returned was (unexpectedly) a java.util.Date.
  Here is a code snippet that fixed my issue...

            # Check if tms is a java.util.Date object...
            # It seems that Boon might return a java.util.Date var if it can convert the TimeStamp field.
            if type(tms).__name__ == 'Date':
                # Convert the java.util.Date to our Standard TimeFormat 'YYY-MM-DDThh:mm:ss.sss'
                dtfmt = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
                tms = dtfmt.format(tms)
                # print "Reformatted tms:[ typeIs[{}] tms:[{}]]".format(type(tms), tms)

  Here's a case where the field returned was a some sort of java-none type, in cases
  when the field requested did not exists in the JSON structure.
  Normally, I expect the value to be a string or a 'unicode' type, so if the field
  does not exists, I want the value 'Unknown' instead...

            # Make sure we have values for class-method strings.
            if type(cls).__name__ != 'unicode':
                cls = "Unknown"
            if type(mth).__name__ != 'unicode':
                mth = "Unknown"

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

from org.boon.Boon import *
from org.boon.json import JsonFactory, JsonSerializerFactory
from java.util     import Map as JMap, HashMap
from java.lang     import Exception as JException
from java.nio.file import Files, Paths, Path

import unittest, sys

''' =================================================================
    Class for Fast JSON Serialization Deserialization 
'''
class BoonJson():
    def __init__ (self):
        self.mapper = JsonFactory.create()
        if (self.mapper is None):
            print "Failed to create Boon Mapper!"

        self.jsonSerializer = JsonSerializerFactory().create() 

    # Deserialize JSON string into a Java Map object.
    def deserFromJson (self, jsonIn):
        boonVal = self.mapper.readValue(jsonIn, JMap)
        return boonVal

    def serToJson (self, jmap):
        jsonStr = self.jsonSerializer.serialize(jmap).toString()
        return jsonStr

    def jsonToObj (self, jsonIn):
        return self.deserFromJson(jsonIn)

    def objToJson (self, jmap):
        return self.serToJson(jmap)

    def readJsonFile (self, filePath = None):
        fp = Paths.get(filePath);
        jsonStr = Files.readAllBytes(fp);
        return self.deserFromJson(jsonStr)

class TestBoonJson(unittest.TestCase):
    def testBoonJson(self):
        boon = BoonJson()

        self.assert_(boon is not None)

        # ======
        # This is an example of going from a JSON string
        # to a Java Map object.
        # ======

        # This is our test JSON string...
        # {
        #    "c" : "002",
        #    "a" : "000",
        #    "b" : 1,
        #    "d" : {
        #       "e" : "xxx",
        #       "f" : [
        #          {
        #             "g" : "yyy"
        #          },
        #          {
        #             "h" : "zzz"
        #          }
        #       ]
        #    }
        # }
        jsonStr = '{"a":"000","b":1,"c":"002","d":{"e":"xxx","f":[{"g":"yyy"},{"h":"zzz"}]}}'
        # Deserialize JSON into a Java Map object.
        boonVal = boon.deserFromJson(jsonStr)
        # print "boonVal.a:[ %s ]" % boonVal.get("a")
        self.assert_(boonVal.get("a") == "000")
        # print "boonVal.b:[ %s ]" % boonVal.get("b")
        self.assert_(boonVal.get("b") == 1)
        # Arrays of Object are supported... Yay!
        # print "boonVal.d.f[1].h:[ %s ]" % boonVal.get("d").get("f")[1].get("h")
        self.assert_(boonVal.get("d").get("f")[1].get("h") == "zzz")

        # ======
        # This is an example of going from a JSON string to a Java Map object
        # and back to a JSOn string.
        # ======

        # Serialize to a JSON string
        newJsonStr = boon.serToJson(boonVal)
        # print "\n==> {}\n".format(newJsonStr)
        expectedJsonStr = '{"a":"000","b":1,"c":"002","d":{"e":"xxx","f":[{"g":"yyy"},{"h":"zzz"}]}}'
        self.assert_(newJsonStr == expectedJsonStr)

    # Test should throw exception if JSON string is not valid JSON.
    def testBoonBadJson (self):
        boon = BoonJson()
        self.assert_(boon is not None)
        # This is our test JSON string...
        # {
        #    "c" : "002",
        #    "a" : "000",
        #    "b" : 1,
        #    "d" : {
        #       "e" : "xxx",
        #       "f" : [
        #          {
        #             "g" : "yyy"
        #          },
        #          {
        #             "h" : "zzz"
        #          }
        #  -x- ] -x- Missing!
        #    }
        # }
        jsonStr = '{"a":"000","b":1,"c":"002","d":{"e":"xxx","f":[{"g":"yyy"},{"h":"zzz"}}}'
        exceptionCaught = False
        try:
            # Deserialize JSON into a Java Map object.
            boonVal = boon.deserFromJson(jsonStr)
            
        except JException as ex:
            # Boon parsing failures are caught as Java Exceptions.
            # Boon is Java so it will throw Java Exceptions.
            # print "\n-1-:Cool! Exception Caught [{}].".format(ex)
            exceptionCaught = True
            
        self.assert_(exceptionCaught == True)

    def testBoonEmbededJson (self):
        ''' Test the the action of capturing a json object as a string within a json structure.
        '''
        boon = BoonJson()
        self.assert_(boon is not None)
        # This is our test JSON string...
        # {
        #    "event" : "<jsonEventString>",
        # }
        jsonStr = '{"a":"000","b":1,"c":"002","d":{"e":"xxx","f":[{"g":"yyy"},{"h":"zzz"}]}}'
        expectedValue = r'{"event":"{\"a\":\"000\",\"b\":1,\"c\":\"002\",\"d\":{\"e\":\"xxx\",\"f\":[{\"g\":\"yyy\"},{\"h\":\"zzz\"}]}}"}'

        boonVal = HashMap()
        boonVal.put('event', jsonStr)
        newJson = boon.serToJson(boonVal)
        # print "newJson:[{}]".format(newJson)
        self.assert_(newJson == expectedValue)

    # Test Serialize: from json string to boon/map object. 
    def testJsonSerializer (self):
        boon = BoonJson()
        self.assert_(boon is not None)
        # Regenerate the jsonEvent after removing the Data object...  
        jsonSerializer = JsonSerializerFactory().create() 
        self.assert_(jsonSerializer is not None)
        jsonStr = '{"a":"000","b":1,"c":"002","d":{"e":"xxx","f":[{"g":"yyy"},{"h":"zzz"}]}}'
        # Deserialize JSON into a Java Map object.
        boonVal = boon.deserFromJson(jsonStr)
        jsonEvent = boon.serToJson(boonVal)
        # print "boonVal.a:[ %s ]" % boonVal.get("a")
        self.assert_(jsonEvent == jsonStr)
        
    def testReadJsonFile (self):
        boon = BoonJson()
        self.assert_(boon is not None)
        boonVal = boon.readJsonFile(filePath = 'conf/table-schemas.json')
        self.assert_(boon is not None)
        self.assert_(boonVal.get("tableList") is not None)
        

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

