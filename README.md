PyAsync
=======

Monadic Asynchronous (deferred execution) library for python based on threading

Notice: This is experimental code, and is provided without warranty, under the
Apache License 2.0. I will not accept any responsibility for damage or loss
whatsoever, even if it is caused in a bug in the library. Be warned!

Documentation
-------------

### The deferred
The deferred is a unit of computation that "promises" a result in the future.
This is logically thought of as a Monad. As such we can think of it as a type
modifier, which modifies the type of the data returned by the original code thunk.
We can then abstract things as 't deferred' where t is the original type.
Unwrapping the deferred logically maps to waiting for the computation to complete.

#### Creating a deferred
You can create a deferred that is already determined from the beginning as such:
```python
from wasync import *
d = determined(1)
```

Or create a deferred that executes a lambda:
```python
from wasync import *
d = deferred(lambda _ : 'foo')
```

If I want to wait for the deferred result:
```python
from wasync import *
d = deferred(lambda _ : 'foo')
print await(d)
```

#### Connecting deferreds: Bind and Chain
If I have a deferred and want to run some code upon completion, and my code should
not be deferred:
```python
from wasync import *
d = deferred(lambda _ : 'foo')
print d.bind(lambda x: 'deferred said {0}'.format(x))
```

If I have a deferred and want to run some code upon completion, and my code should
be deferred automatically:
```python
from wasync import *
d = deferred(lambda _ : 'foo')
print d.chain(lambda x: 'deferred said {0}'.format(x))
```

#### Infix notation and functions
These are equivalent for chaining:
```python
d.chain(lambda x: f(x))
d |c| f
```
For binding:
```python
d.bind(lambda x: f(x))
d |b| f
```

If you combine these with functions, the pattern becomes a lot more powerful, in a
way that also looks similar to s/ML:
```python
def f(x):
    y = do_something_to(x)
    print(x)
    return y
d |c| f |c| g |c| ...
```
