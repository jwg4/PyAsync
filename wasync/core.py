# $Id: core.py 37114 2014-08-20 11:18:43Z stuartf $
import threading
import concurrent.futures        
import Queue as Q
import traceback
import gc
import scheduler
import signal
from raw_deferred import Raw_Deferred
__version__ = '$Rev$'
import sys


_scheduler = None
_go_future = None

def shutdown():
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown()
    _scheduler = None

def shutdown_on_signal(signum=None,frame=None):
    shutdown()

def go(threads = scheduler.MIN_THREADS, debug = None):
    global _scheduler 
    global _go_future
    signal.signal(signal.SIGINT,shutdown_on_signal)
    if sys.platform.startswith('linux'):
        signal.signal(signal.SIGALRM,shutdown_on_signal)
        signal.signal(signal.SIGHUP,shutdown_on_signal)
    if _scheduler is None:
        _scheduler = scheduler.Scheduler(threads,debug)
        _go_future = _scheduler.go(debug)
    return _go_future

def never_returns(threads = scheduler.MIN_THREADS, debug = None):
    return go(threads,debug).result()

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

def deferred(f=None):
    d = Raw_Deferred(f)
    _scheduler.submit_job(d)
    return d

def determined(x):
    """Make a deferred from a value x, bypassing the scheduler. x will never be called.
    """
    d = Raw_Deferred()
    d.determine(x)
    return d

def determined_list(l):
    return [determined(x) for x in l]

def auto_defer(o):
    """Auto-deferring of objects and functions"""
    if isinstance(o, Raw_Deferred):
        r = o
    else:
        if hasattr(o, '__call__'):
            r = deferred(o)
        else:
            r = determined(o)
    return r

def await(d):
    return d.await_result()

#'a Def -> (f: 'a -> 'b) -> 'b
def bind(d,f):
    return f(await(d))

#'a Def -> (f: 'a -> 'b) -> 'b Def
def chain(d,f):
    d2 = Raw_Deferred(lambda d=d,f=f: f(d.result))
    #using a callback does not exhaust threads waiting
    d2.add_blocker(d)
    d.add_callback(lambda v,d2=d2: _scheduler.submit_job(d2))
    return d2
#        n = Raw_Deferred(None)
#        #this is OK because it runs inside the worker thread
#        self.add_callback(lambda v=self : n.determine(f(v.result)))
#        return n

#'a Def list -> 'a list
def await_all(deferreds):
    return [await(d) for d in deferreds]

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
    return deferred(lambda: bind_all(deferreds,function))

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
    return deferred.bind(determined_list)
    
#'a Def list -> 'a list Def
def join(deferreds):
    return chain_all(deferreds,lambda x: x)

def bind_or_apply(maybe_deferred,function):
    if isinstance(maybe_deferred, Raw_Deferred):
        return maybe_deferred.bind(function)
    else:
        return function(maybe_deferred)

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

def sleep(time):
    threading.sleep(time)
    return defined(None)

def every(time,f):
    def loop():
        d = sleep(time).chain(f)
        d.chain(loop)

class Queue:

    def __init__(self):
        self._objects = Q.Queue()

    def put(self,val):
        return deferred(lambda val=val: self._objects.put(val))

    def get(self):
        return deferred(lambda: self._objects.get(True))
    
