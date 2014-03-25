# $Id: core.py 34068 2014-03-25 12:19:05Z alanm $
import threading
import concurrent.futures        
import Queue as Q
import traceback
__version__ = '$Rev$'

_scheduler = None
#TODO: auto-detect some metric
DEFAULT_THREADS = 200

class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __rshift__(self, other):
        return self.function(other)
    def __call__(self, value1, value2):
        return self.function(value1, value2)

def start(threads=DEFAULT_THREADS):
    global _scheduler
    if not _scheduler:
        _scheduler = concurrent.futures.ThreadPoolExecutor(max_workers=threads)

def shutdown():
    global _scheduler
    _scheduler.shutdown()
    _scheduler = None

class Deferred:
  
    def __init__(self,function):
        self._f = function
        self._future = _scheduler.submit(function)

    def result_opt(self):
        if not self._future.done():
            return None
        else:
            return self._future

    def abort():
        self._future.cancel()

    #'a Def -> 'a
    def await_result(self):
        try:
            r = self._future.result()
            return r
        except Exception as e:
            print "Exception " + str(e) + " in module " + str(self._f.__module__) + ", function " + self._f.__name__
            print str(self._f.__code__.co_filename) + ", line "  + str(self._f.__code__.co_firstlineno)
            raise
  
    #'a Def -> (f: 'a -> 'b) -> 'b
    def bind(self,function):
        return function(self.await_result())

    #'a Def -> (f: 'a -> 'b) -> 'b Def
    def chain(self,function):
        return Deferred(lambda: self.bind(function))

def defined(x):
    return Deferred(lambda: x)

def defined_list(l):
    return [defined(x) for x in l]

def await(deferred):
    return deferred.await_result()

#'a Def list -> 'a list
def await_all(deferreds):
    return [deferred.await_result() for deferred in deferreds]

def await_first(deferreds):
    if len(deferreds) < 1:
        return None 
    closure = {'result' : concurrent.futures.Future()}
    def helper(v):
        if not closure['result'].done():
            closure['result'].set_result(v)
    new_deferreds = [d.chain(lambda v: helper(v)) for d in deferreds]
    return closure['result'].result()

#'a Def -> 'b Def -> f('a->'b->'c) -> 'c
def bind2(a,b,function):
    return function(a.await_result(), b.await_result())

#'a Def list -> (f: 'a -> 'b list) -> 'b list
def bind_all(deferreds,function):
    return function(await_all(deferreds))

#'a Def list -> (f: 'a -> 'b list) -> 'b list Def
def chain_all(deferreds,function):
    return Deferred(lambda: bind_list(deferreds,function))

#'a Def list -> (f: 'a -> 'b) -> 'b list
def bind_each(deferreds,function):
    return [deferred.bind(function) for deferred in deferreds]

#'a Def list -> (f: 'a -> 'b) -> 'b list Def
def chain_each(deferreds,function):
    return [deferred.chain(function) for deferred in deferreds]

#'a Def list -> 'b acc (f: 'b -> 'a -> 'b) -> 'b
def fold(deferreds,init,function):
    acc = init
    for deferred in deferreds:
        acc = deferred.bind(lambda x,acc=acc: function(acc,x))
    return acc

#'a list Def -> 'a Def list
def disjoin(deferred):
    return deferred.bind(defined_list)
    
#'a Def list -> 'a list Def
def join(deferreds):
    return defined(await_all(deferreds))
    
def select(deferreds):
    if len(deferreds) < 1:
        return
    closure = { 'left' : len(deferreds), 'values' : Q.Queue() }
    def helper(v):
        closure['left'] = closure['left'] - 1
        closure['values'].put(v)
    for d in deferreds:
        d.chain(lambda v: helper(v))
    while(closure['left'] > 0 or not closure['values'].empty()):
        yield closure['values'].get()

class Queue:

    def __init__(self):
        self._objects = Q.Queue()

    def put(self,val):
        return Deferred(lambda val=val: self._objects.put(val))

    def get(self):
        return Deferred(lambda: self._objects.get(True))
    
