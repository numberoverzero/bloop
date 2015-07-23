.. _quickstart:

Quickstart
==========

Create a model and bind it to a DynamoDB Table (creating one if it doesn't
exist):

.. literalinclude:: code/quick.py
    :language: python
    :lines: 1-18

Create a user, and save with a condition to ensure a unique id:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 20-27

Modify a field, then save changes back to DynamoDB (only modified attributes
are sent):

.. literalinclude:: code/quick.py
    :language: python
    :lines: 29-31

We can create, save, and load multiple objects at once, even different models:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 34-45

Both save and delete take optional conditions, which can be used to ensure
certain attributes have expected values (or lack of values) before the given
changes are persisted:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 47-57

What about making sure ALL of the loaded values are the same?  We'd have to
construct a condition for every attribute, and then AND them together:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 60-65

Don't forget we need to handle cases where the tweet doesn't have any content,
and building the condition gets more complex.  What about if the tweet's
content is empty, but the tweet was loaded from a query against an index that
doesn't project content?  We shouldn't expect it to be empty or not-empty,
because we never tried to load it!

There's an easier way:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 67-68

We can also set the engine to always use atomic conditions, with:

.. literalinclude:: code/quick.py
    :language: python
    :lines: 70

The context version allows us to temporarily talk through the engine as if it
had the given config values, without changing the engine's actual config values
for other callers.

For more details about constructing conditions, see :ref:`conditions`.

For more details about available types, see :ref:`types`.

For details about other config settings, see :ref:`prefetch-strict` and
:ref:`persist`.
