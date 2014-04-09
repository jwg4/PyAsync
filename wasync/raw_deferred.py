# $Id: raw_deferred.py 34456 2014-04-08 18:27:00Z eda $
import core
import threading

class Raw_Deferred:
    """A unit of computation that runs in the background, which immediately returns a 
    promise that will be fulfilled at a later time."""
  
    def __init__(self,function=None):
        self.function = function
        self.callbacks = []
        self.result = None
        self.determination = threading.Event()

    def determine(self,val):
        self.result = val
        self.determination.set()

    def add_callback(self,f):
        if self.determination.is_set():
            f(self.result)
        else:
            self.callbacks.append(f)

    def result_opt(self):
        if not self.determination.is_set():
            return None
        else:
            return self.result

    def await(self):
            self.determination.wait()
            return self.result

    def await_result(self):
        return self.await()

    def bind(self,f):
        return core.bind(self,f)

    def chain(self,f):
        return core.chain(self,f)
