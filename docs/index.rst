Bloop: Better living through declarative modeling
=================================================

DynamoDB_ is great.  Unfortunately, taking advantage of its many options
through the `boto3 client`_ requires a `significant amount of code`_ for things
that `should be simple`_.


It doesn't need to be like this.

Bloop offers SQLAlchemy-inspired declarative modeling that cuts down the
overhead of talking in boto3's endlessly nested dicts, instead letting you
take advantage of advanced DynamoDB features like `conditional saves`_ and
`atomic updates`_ with just a few lines.


Features
--------

Bloop supports the latest DynamoDB features, including:

* All DynamoDB `data types`_ as well as sane implementations of DateTime
  and UUID
* Local and Global `Secondary Indexes`_
* `Conditional Writes`_ for Save and Delete operations
* `Expressions`_ which allow the use of reserved words for attribute names

User Guide
----------

.. toctree::
   :maxdepth: 2

   installation
   getting_started
   advanced_customization

API Documentation
-----------------

The bloop API is separated into public and private components.  The public API
follows semver, and can be considered stable within a major release.  The
private API can change at any time and is provided as a reference for bloop
developers.

.. toctree::
   :maxdepth: 2

   public_api
   internal_api

Contributing
------------

.. toctree::
    :maxdepth: 1

    contributing

.. _DynamoDB: http://aws.amazon.com/dynamodb/
.. _boto3 client: http://boto3.readthedocs.org/en/latest/reference/services/dynamodb.html
.. _significant amount of code: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-boto3-py
.. _should be simple: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-bloop-py
.. _conditional saves: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-conditional_save-py
.. _atomic updates: https://gist.github.com/numberoverzero/c0fb8c521cac7bb4abe7#file-atomic_update-py
.. _data types: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DataModel.html#DataModel.DataTypes
.. _Secondary Indexes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
.. _Conditional Writes: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate
.. _Expressions: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.html
