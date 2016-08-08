.. _types:

Types
^^^^^

========
Built-in
========

String, Float, etc.

============
Custom Types
============



----------------
Missing and None
----------------

None is missing, etc etc.  Return None to omit.

--------------
``bloop.Type``
--------------

.. code-block:: python

    class Type:
        backing_type = "S"

        def dynamo_load(self, value, *, context, **kwargs):
            return value
        def dynamo_dump(self, value, *, context, **kwargs):
            return value

------------
Generic Enum
------------

String-based Enum
