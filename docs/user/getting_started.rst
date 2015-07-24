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

Note that we saved two users at once.  ``load``, ``save``, and ``delete`` take
either a single instance of a model, or an iterable of models.  It's safe
(although unnecessary) to include the same instance (or two instances with
equivalent keys) more than once.

We're currently only working with a single model ``User``, but instances of
different models can be modified in the same call - there is no need to batch
changes by model type.

Query and Scan
--------------

We can construct queries and scans against tables or an index.  Since the
syntax for queries and scans is the same, we'll just be using queries in the
following examples.  Keep in mind that specifying a Key condition is required
for a query, while it's ignored for a scan.

.. literalinclude code/query1_repl.txt

Queries can be built iteratively by chaining function calls and properties,
which means we can build a base query and then construct more specific queries
from that:

.. literalinclude code/query2_repl.txt

The following properties are exposed::

    ascending - set the ScanIndexForward parameter to True
    descending - set the ScanIndexForward parameter to False
    consistent - set the ConsistentRead parameter to True
                 NOTE: ConsistentRead cannot be used with a GSI.

The ``.key(condition)`` function will generate a KeyCondition.  When querying
a table or index that has both a hash and range key, a condition for BOTH must
be specified, with an AND::

    query(User).key((User.first == 'first name') &
                    (User.last == 'last_name'))
    query(User.by_account).key((User.balance != 0.5) &
                               (User.credits >= 3))

The ``.filter(condition)`` function will generate a FilterCondition.  This is
applied after the key condition, and filters results server-side.  Iterative
filters will be ANDed together::

    query = (query(User).key(...)
                        .filter(User.credits > 1)
                        .filter(User.admin.is_(True)))

The ``.select(columns)`` function takes either ``'all'``, ``'projected'``,
or a list of Column objects::

    all:
        load all attributes if querying a table.  If querying a GSI,
        loads all attributes projected into the index.  If querying
        a LSI in strict mode, this will raise if the index does not
        have an ALL projection.  When querying a LSI without strict
        mode, the query will incur additional reads against the
        table to load all atrributes.

    projected:
        load all attributes projected into the index.  This is only a
        valid option if querying an index.

    list of columns:
        load only the specified columns.  When querying a GSI, (or
        a LSI in strict mode) this will raise if the columns are not
        all available from the projection.  If querying a LSI without
        strict mode and the set of columns is not available from the
        LSI projection, the query will incur additional reads against
        the table to load the requested attributes.

Results can be collected from ``.all(prefetch=None)`` or ``.first()`` or
by iterating the query.  To re-execute the query, user the iterator.  To
iterate over the results of a single execution, use ``.all``.

.. _conditions:

Conditions
----------

condition=Model.column == value

rich comparisons

ensure unique id

Atomic
------

ensure no changes since load

.. _types:

Types
-----

arrow.Arrow, UTC everywhere, rich comparisons
