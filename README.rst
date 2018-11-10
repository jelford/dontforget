dontforget
==========

.. image:: https://travis-ci.org/jelford/dontforget.svg?branch=master
    :target: https://travis-ci.org/jelford/dontforget

.. image:: https://img.shields.io/pypi/v/dontforget.svg?maxAge=2592000
    :target: https://pypi.python.org/pypi?:action=display&name=dontforget

A decorator to keep results around for later

.. code-block:: python

    >>> from dontforget import cached
    >>> from time import sleep
    >>> 
    >>> @cached
    ... def long_running():
    ...     print('Doing an expensive API call...')
    ...     sleep(5)
    ...     return 42
    ... 
    >>> long_running()
    Doing an expensive API call...
    42
    >>> long_running()
    42

Description
-----------

``dontforget`` provides a caching decorator that you can apply to your
long-running functions to keep their outputs around for later. Results
are saved to local disk, so they'll still be around when you come back.

``dontforget`` is like ``functools.lru_cache()`` except that you get to
keep the results around between runs. It comes in handy when you have
expensive computation or data loading to perform as part of your pipeline,
but the results are unlikely to change between one run and the next.

``dontforget`` is smart enough to bust your cache when:

* Your arguments change - hopefully that goes without saying; or
* Your function body changes - so you aren't re-using results from a computation you no longer care about

``dontforget`` also accepts a cache-busting key to enable you to flush
state between releases, or whenever you feel like it.
  
Use cases
---------

Consider using ``dontforget`` when:

* You want to keep results around *between* runs - so an ``lru_cache`` won't cut it
* You are willing to trade disk space for speed
* You have pure functions whose results don't change between invocations.

Don't consider using ``dontforget`` when:

* You call out to a live data source and don't want stale results
* Your disk won't live long enough to use the data you've hived off

