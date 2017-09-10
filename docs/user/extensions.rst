Bloop Extensions
^^^^^^^^^^^^^^^^

Extension dependencies aren't installed with Bloop, because they may include a huge number of libraries that Bloop
does not depend on.  For example, two extensions could provide automatic mapping to Django or SQLAlchemy models.
Many users would never need either of these, since Bloop does not depend on them for normal usage.

Bloop extensions are part of the :ref:`Public API <public-extensions>`, and subject to
:ref:`its versioning policy<meta-versioning-public>`.

.. _user-extensions-datetime:

==========
 DateTime
==========

Working with python's :class:`datetime.datetime` is tedious, but there are a number of popular libraries
that improve the situation.  Bloop includes drop-in replacements for the basic
:class:`~bloop.types.DateTime` type for `arrow`_, `delorean`_, and `pendulum`_ through the
:ref:`extensions module<public-ext-datetime>`.  For example, let's swap out some code using the built-in DateTime:

.. code-block:: python
    :emphasize-lines: 1, 2, 9, 10

    import datetime
    from bloop import DateTime
    from bloop import BaseModel, Column, Integer

    class User(BaseModel):
        id = Column(Integer, hash_key=True)
        created_on = Column(DateTime)

    utc = datetime.timezone.utc
    now = datetime.datetime.now(utc)

    user = User
        id=0,
        created_on=now
    )

Now, using pendulum:

.. code-block:: python
    :emphasize-lines: 1, 2, 9

    import pendulum
    from bloop.ext.pendulum import DateTime
    from bloop import BaseModel, Column, Integer

    class User(BaseModel):
        id = Column(Integer, hash_key=True)
        created_on = Column(DateTime)

    now = pendulum.now("utc")

    user = User
        id=0,
        created_on=now
    )

Now, using arrow:

.. code-block:: python
    :emphasize-lines: 1, 2, 9

    import arrow
    from bloop.ext.arrow import DateTime
    from bloop import BaseModel, Column, Integer

    class User(BaseModel):
        id = Column(Integer, hash_key=True)
        created_on = Column(DateTime)

    now = arrow.now("utc")

    user = User
        id=0,
        created_on=now
    )

..
.. _arrow: http://crsmithdev.com/arrow
.. _delorean: https://delorean.readthedocs.io/en/latest/
.. _pendulum: https://pendulum.eustace.io
