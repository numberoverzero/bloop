Getting Started
===============

Although this model may seem daunting at first, we'll be using it throughout
this and later guides to demonstrate all of the available options, and how they
affect the core engine functions.  By the end it should be easy to follow, and
be a fair improvement over the `boto3 equivalent`_.

To help follow along, you can copy the model below from the `examples folder`_.


.. _boto3 equivalent: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-getting_started_raw-py
.. _examples folder: https://github.com/numberoverzero/bloop/blob/master/examples/getting_started.py

Create a Model
--------------

engine.bind()

Save and Delete Objects
-------------------------

engine.save, engine.delete

Query and Scan
--------------

engine.query, engine.scan

Conditions
----------

condition=Model.column == value

rich comparisons

ensure unique id

Atomic
------

ensure no changes since load

Types
-----

arrow.Arrow, UTC everywhere, rich comparisons
