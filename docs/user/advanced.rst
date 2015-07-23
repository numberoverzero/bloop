Advanced Features
=================

.. _prefetch-strict:

Prefetch and Strict
-------------------

engine.config['prefetch'] = 'all'

.. _persist:

Persist Mode
------------

update vs overwrite

.. _engine-contexts:

Engine Contexts
---------------

with engine.context(atomic=True, persist='overwrite') as atomic:

Session Profiles
----------------

Pass a session to an Engine to use a named profile, change the region, etc.

Custom Types
------------

subclass bloop.types.Type

Custom Columns
--------------

subclass bloop.column.Column

Custom Object Loading
---------------------

bloop_init

Declarative Models
------------------

Models don't support inheritance
