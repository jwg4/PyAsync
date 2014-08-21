# $Id: raw_deferred.py 37041 2014-08-18 13:32:15Z alanm $
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
        self.blockers = []

    def determine(self,val):
        self.result = val
        self.determination.set()

    def is_blocked(self):
        if len(self.blockers) == 0:
            return False
        else:
            blocked = False
            for b in self.blockers:
                if isinstance(b,Raw_Deferred):
                    if b.is_blocked():
                        blocked = True

    def add_callback(self,f):
        if self.determination.is_set():
            f(self.result)
        else:
            self.callbacks.append(f)

    def add_blocker(self,d):
        self.blockers.append(d)
        
    def peek_opt(self):
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
