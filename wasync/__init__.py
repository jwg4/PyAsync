from core import Infix
import network 

## Here comes the syntactic sugar ##

b = Infix(lambda a,f: bind(a,f))
c = Infix(lambda a,f: chain(a,f))
ba = Infix(lambda d,f: bind_all(d,f))
be = Infix(lambda d,f: bind_each(d,f))
ca = Infix(lambda d,f: chain_all(d,f))
ce = Infix(lambda d,f: chain_each(d,f))
boa = Infix(lambda d,f: bind_or_apply(d,f))

## Only export these ##
__all__ = [
    'await',
    'await_all',
    'await_all_dict',
    'await_first',
    'bind2',
    'bind',
    'chain',
    'bind_all',
    'bind_all_dict',
    'chain_all',
    'chain_all_dict',
    'bind_each',
    'chain_each',
    'deferred',
    'b',
    'c',
    'be',
    'ba',
    'ce',
    'ca',
    'go',
    'shutdown',
    'never_returns',
    'fold',
    'determined',
    'determined_list',
    'join',
    'join_dict',
    'disjoin',
    'bind_or_apply',
    'sleep',
    'every'
]
