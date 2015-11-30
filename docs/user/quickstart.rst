Quickstart
==========

Continuing from the model code on the front page:

.. literalinclude:: ../code/models.py

Create a user, and save with a condition to ensure a unique id:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 5-10

Modify a field, then save changes back to DynamoDB:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 12-14

You can create, save, and load multiple objects at once, even different models:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 16-21

Both save and delete take optional conditions, which can be used to ensure
certain attributes have expected values (or lack of values) before the given
changes are persisted:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 23-26

What about making sure ALL of the loaded values are the same?  We'd have to
construct a condition for every attribute, and then AND them together:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 28-33

Don't forget to handle cases where the tweet doesn't have any content, and
building the condition gets more complex.  What about if the tweet's
content is empty, but the tweet was loaded from a query against an index that
doesn't project content?  There shouldn't be an expectation of empty or
not-empty, because it was never loaded!

There's an easier way:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 35-38

Note that atomic modifications require the object to have been loaded through
an atomic context; otherwise, the initial state won't be preserved.  This is
because constructing and storing the initial state on every load is expensive,
and therefore not done for non-atomic loads/queries.

You can also set the engine to always use atomic conditions, with:

.. literalinclude:: ../code/quick.py
    :language: python
    :lines: 40

The context version allows you to temporarily talk through the engine as if it
had the given config values, without changing the engine's actual config values
for other callers.

.. seealso::
    * For more details about constructing conditions, see :ref:`conditions`.
    * For more details about available types, see :ref:`types`.
