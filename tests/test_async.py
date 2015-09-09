import wasync
from wasync import *
import datetime as dt
import time
import sys
import unittest

sys.path.append(".")

def longfunc(x):
    time.sleep(x)
    return x

class TestBasicFunctionality(unittest.TestCase):
    
    def setUp(self):
        self.scheduler = wasync.go()

    def testDeferredAwait(self):
        f1 = deferred(lambda: longfunc(0.1))
        f1.await()
        self.assertEqual(f1.await(),0.1,'Deferred.await() is broken')

    def testCoreAwait(self):
        f1 = deferred(lambda: longfunc(0.1))
        self.assertEqual(await(f1),0.1,'await() is broken')
    
    def testConcurrency(self):
        start = time.time()
        f1 = deferred(lambda: longfunc(0.2))
        f2 = deferred(lambda: longfunc(0.4))
        f1.await()
        after_f1 = time.time()
        f2.await()
        after_f2 = time.time()
        self.assertLess(after_f1 - start, 0.3, 'concurrency is broken') 
        self.assertLess(after_f2 - start, 0.6, 'concurrency is broken') 
        f1 = deferred(lambda: longfunc(0.2))
        f2 = deferred(lambda: longfunc(0.4))
        self.assertEqual(await_first([f1,f2]),0.2,'await_first() is broken')
        self.assertListEqual(await_all([f1,f2]),[0.2,0.4],'await_all() is broken')

    def testBind(self):
        f1 = determined(1)
        f2 = f1.bind(lambda x: x + 1)
        self.assertEqual(f2,2)

    def testChain(self):
        f1 = determined(1)
        f2 = f1.chain(lambda x: x + 1)
        self.assertEqual(f2.await(),2)

if __name__ == '__main__':
    unittest.main()
