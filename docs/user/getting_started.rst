Getting Started
===============

It's recommended that you copy the `user model code`_ below and follow along.

.. _user model code: https://github.com/numberoverzero/bloop/blob/master/docs/user/code/user.py

Create a Model
--------------

We'll be using this model throughout this and later guides to demonstrate all
of the available options, and how they affect the core engine functions.  By
the end it should be easy to follow, and be a fair improvement over the
`boto3 equivalent`_ to generate this table.

.. _boto3 equivalent: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-getting_started_raw-py

.. literalinclude:: code/user.py
    :language: python
    :lines: 1-32
    :linenos:

We'll compare the generated code when using atomic updates, using ``overwrite``
vs ``update``, and when querying different Indexes.

Load, Save, and Delete
----------------------

Because our model has hash and range keys, we'll need to specify both to save
or load an instance of the model.

We'll be creating and deleting users quite a bit - to make things simpler,
let's wrap the creation in a function.  Now we can regenerate some users with a
simple ``create_users()``.

.. literalinclude:: code/user.py
    :lines: 35-59
    :linenos:

With that and the above model saved in ``user.py``, let's open a REPL and
load a user:

.. literalinclude:: code/load_repl.txt

And delete a user.  Trying to load that user after deletion will raise an
exception:

.. literalinclude:: code/delete_repl.txt

We can fix this by saving ``also_richard`` back again.  However, we need to
use an overwrite instead of an update, since that object still thinks it has
the latest version:

.. literalinclude:: code/save_repl.txt

(Don't worry about the ``engine.context`` for now - this is covered later in
:ref:`engine-contexts`)

Note that we saved two users at once.  ``load``, ``save``, and ``delete`` take
either a single instance of a model, or an iterable of models.  It's safe
(although unnecessary) to include the same instance (or two instances with
equivalent keys) more than once.

We're currently only working with a single model ``User``, but instances of
different models can be modified in the same call - there is no need to batch
changes by model type.

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
