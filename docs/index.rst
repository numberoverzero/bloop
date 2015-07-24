Better living through declarative modeling
=================================================

DynamoDB is great.  Unfortunately, it requires some `tricky code`_ for common
operations.  **It doesn't need to be like this**.

Bloop's declarative modeling enables `simpler code`_, while still
exposing advanced DynamoDB features like `conditional saves`_ and
`atomic updates`_.

.. warning::
    While fully usable, bloop is still pre-1.0 software and has **no**
    backwards compatibility guarantees until the 1.0 release occurs!

----

Define some models:

.. literalinclude:: index.py
    :lines: 1-26

Create an instance:

.. literalinclude:: index.py
    :lines: 28-35

Query or scan by column values:

.. literalinclude:: index.py
    :lines: 38-46

.. toctree::
    :hidden:
    :titlesonly:

    user/installation
    user/quickstart
    user/getting_started
    user/engine
    user/models
    user/advanced
    dev/contributing

.. _tricky code: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-query_boto3-py
.. _simpler code: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-query_bloop-py
.. _conditional saves: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-conditional_save-py
.. _atomic updates: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-atomic_update-py
