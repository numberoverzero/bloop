Public API
==========

.. note::

    bloop follows semver for its public api, which means you can rely on the
    following classes to be stable between major releases.

    While the **interface** of objects returned by public classes and methods
    will remain stable (they won't lose or gain public members or functions),
    their implementation or even class can change between minor releases.
    You SHOULD NOT rely on their type or internal members to be stable.

Engine
---------------------

.. autoclass:: bloop.engine.Engine
    :members:
    :undoc-members:

Column
---------------------

.. autoclass:: bloop.column.Column
    :members:
    :undoc-members:

GlobalSecondaryIndex
-----------------------------------

.. autoclass:: bloop.GlobalSecondaryIndex
    :members:
    :undoc-members:

LocalSecondaryIndex
----------------------------------------

.. autoclass:: bloop.index.LocalSecondaryIndex
    :members:
    :undoc-members:

Query
---------------------------

.. autoclass:: bloop.filter.Query
    :members:
    :undoc-members:

Scan
--------------------------

.. autoclass:: bloop.filter.Scan
    :members:
    :undoc-members:

Condition
------------------------

.. autoclass:: bloop.condition.Condition
    :members:
    :undoc-members:

Types
------------------

.. automodule:: bloop.types
    :members:
    :undoc-members:
    :show-inheritance:

Exceptions
-----------------------

.. automodule:: bloop.exceptions
    :members:
    :undoc-members:
    :show-inheritance:
