import core
from core import await, await_all, await_first, bind2, bind_all, chain_all, bind_each, chain_each, Deferred, Queue, start, select, fold, defined, defined_list, join, disjoin

## Here comes the syntactic sugar ##

b = core.Infix(lambda a,f: Deferred.bind(a,f))
c = core.Infix(lambda a,f: Deferred.chain(a,f))
ba = core.Infix(lambda d,f: bind_all(d,f))
be = core.Infix(lambda d,f: bind_each(d,f))
ca = core.Infix(lambda d,f: chain_all(d,f))
ce = core.Infix(lambda d,f: chain_each(d,f))

## Only export these ##
__all__ = [
    'await',
    'await_all',
    'await_first',
    'bind2',
    'bind_all',
    'chain_all',
    'bind_each',
    'chain_each',
    'Deferred',
    'b',
    'c',
    'be',
    'ba',
    'ce',
    'ca',
    'start',
    'fold',
    'defined',
    'defined_list',
    'join',
    'disjoin'
]
