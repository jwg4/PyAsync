# $Id: Async.py 33672 2014-03-05 17:40:48Z eda $
import gevent
import gevent.event
import threading
import Queue as Q
__version__ = '$Rev$'

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

class Deferred:
  
  def __init__(self,function):
    self._finished = False
    self._value = None
#TODO: use AsyncResult instead of Event
    self._waker = gevent.event.Event()
    self._job = gevent.spawn(self.handler,function)
    self._callbacks = []

  def handler(self,function):
    self._value = function()
    self._finished = True
    for callback in self._callbacks:
        callback(self._value)
    self._waker.set()

  def result_opt(self):
    if not self._finished:
        return None
    else:
        return self._value

  #'a Def -> 'a
  def await_result(self):
    self._waker.wait()
    return self._value
  
  #'a Def -> (f: 'a -> 'b) -> 'b
  def bind(self,function):
    return function(self.await_result())

  #'a Def -> (f: 'a -> 'b) -> 'b Def
  def chain(self,function):
    return Deferred(lambda: function(self.await_result()))

#'a Def list -> 'a list
def await_all(deferreds):
  [deferred.await_result() for deferred in deferreds]

#'a Def -> 'b Def -> f('a->'b->'c) -> 'c
def bind2(a,b,function):
  return function(a.await_result(), b.await_result())

#'a Def list -> (f: 'a -> 'b) -> 'b list
def bind_list(deferreds,function):
  return function(awaitall(deferreds))

def await_first(deferreds):
  waker = gevent.event.AsyncResult()
  def helper(deferred):
    v = deferred.await_result()
    if not waker.ready():
        waker.set(v)
  new_deferreds = [Deferred(lambda: helper(deferred)) for deferred in deferreds]
  return waker.get()

## Here comes the syntactic sugar ##

b = Infix(lambda a,f: Deferred.bind(a,f))
c = Infix(lambda a,f: Deferred.chain(a,f))
b2 = Infix(lambda a,b,f: bind2(a,b,f))
bl = Infix(lambda d,f: bind_list(d,f))


## Handy async data structures ##

class Queue:

  def __init__(self):
    self._objects = Q.Queue()

  def put(self,val):
    self._objects.put(val)

  def get(self):
    return Deferred(self._objects.get)
    
