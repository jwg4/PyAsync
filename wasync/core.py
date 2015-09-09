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
    """Gracefully close the Wasync scheduler and stop submitting new threads"""
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown()
    _scheduler = None

def shutdown_on_signal(signum=None,frame=None):
    """Signal handler function for shutting down"""
    shutdown()

def go(threads = scheduler.MIN_THREADS, debug = None):
    """Start a Wasync scheduler"""
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
    """Start the Wasync scheduler and hang"""
    return go(threads,debug).result()

#needed for syntactic infix sugar
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
    """Create a deferred operation"""
    d = Raw_Deferred(f)
    _scheduler.submit_job(d)
    return d

def determined(x):
    """Make a deferred from a value x, bypassing the scheduler. x will never be called."""
    d = Raw_Deferred()
    d.determine(x)
    return d

def determined_list(l):
    """Make a list of determined() deferreds from a list of values"""
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
    """Wait for a deferred to complete and return its value"""
    return d.await_result()

#'a Def -> (f: 'a -> 'b) -> 'b
def bind(d,f):
    """Apply f to the return value of d in a blocking fashion"""
    return f(await(d))

#'a Def -> (f: 'a -> 'b) -> 'b Def
def chain(d,f):
    """Return a deferred with the result of applying f to the result of d"""
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
    """Wait for a list of deferreds to complete and return the list of return values"""
    return [await(d) for d in deferreds]

def await_all_dict(deferred_dict):
    """Wait for a dictionary of deferreds to complete and return the mapped dictionary of return values"""
    return {k: await(v) for k,v in deferred_dict.iteritems()}

def await_first(deferreds):
    """Wait for the first deferred in a list to complete and return the value, ignoring all others"""
    if len(deferreds) < 1:
        return None 
    closure = {'result' : Raw_Deferred()}
    def helper(v):
        if not closure['result'].is_determined():
            closure['result'].determine(v)
    new_deferreds = [d.chain(lambda v: helper(v)) for d in deferreds]
    return closure['result'].result()

#'a Def -> 'b Def -> f('a->'b->'c) -> 'c
def bind2(a,b,function):
    """Wait for two deferreds to complete and return the result of applying f to them"""
    return function(a.await_result(), b.await_result())

#'a Def list -> (f: 'a -> 'b list) -> 'b list
def bind_all(deferreds,function):
    """bind, applied to a list"""
    return function(await_all(deferreds))

def bind_all_dict(deferred_dict,function):
    """bind, applied to a dictionary"""
    return {k: function(await(v)) for k,v in deferred_dict.iteritems()}

#'a Def list -> (f: 'a -> 'b list) -> 'b list Def
def chain_all(deferreds,function):
    """chain, applied to a list"""
    return deferred(lambda deferreds=deferreds,function=function: bind_all(deferreds,function))

def chain_all_dict(deferred_dict,function):
    """chain, applied to a dictionary"""
    return deferred(lambda deferred_dict=deferred_dict,function=function: bind_all_dict(deferred_dict,function))
 
#'a Def list -> (f: 'a -> 'b) -> 'b list
def bind_each(deferreds,function):
    """bind f internally to each element of a list"""
    return [deferred.bind(function) for deferred in deferreds]

#'a Def list -> (f: 'a -> 'b) -> 'b list Def
def chain_each(deferreds,function):
    """chain f internally to each element of a list"""
    return [deferred.chain(function) for deferred in deferreds]

#'a Def list -> 'b acc (f: 'b -> 'a -> 'b) -> 'b
def fold(deferreds,init,function):
    """accumulate a result over a list of deferreds - conceptually a reduce operation"""
    acc = init
    for deferred in deferreds:
        acc = deferred.bind(lambda x,acc=acc: function(acc,x))
    return acc

#'a list Def -> 'a Def list
def disjoin(deferred):
    """Return a list of deferreds from a deferred list"""
    return deferred.bind(determined_list)
    
#'a Def list -> 'a list Def
def join(deferreds):
    """Return a deferred list from a list of deferreds"""
    return chain_all(deferreds,lambda x: x)

def join_dict(deferred_dict):
    """Return a mapped deferred dictionary from a dictionary of deferreds"""
    return chain_all_dict(deferred_dict,lambda x: x)

def bind_or_apply(maybe_deferred,function):
    """bind or apply a function based on whether the other argument is deferred or not"""
    if isinstance(maybe_deferred, Raw_Deferred):
        return maybe_deferred.bind(function)
    else:
        return function(maybe_deferred)

def select(deferreds):
    """select from a list of deferreds and return the next available"""
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
    """block for a certain amount of time"""
    threading.sleep(time)
    return defined(None)

def every(time,f):
    """run f at discrete intervals"""
    def loop():
        d = sleep(time).chain(f)
        d.chain(loop)

class Queue:
    """A Queue that can be iterated in a deferred fashion"""

    def __init__(self):
        self._objects = Q.Queue()

    def put(self,val):
        return deferred(lambda val=val: self._objects.put(val))

    def get(self):
        return deferred(lambda: self._objects.get(True))
