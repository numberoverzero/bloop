About
^^^^^

============
Contributing
============

Thanks for contributing!  Feel free to `open an issue`_ for any bugs, typos, unhelpful docs, or general unhappiness
which you may encounter while using Bloop.  If you want to `create a pull request`_, even more awesome!  Please
make sure all the non-integ tox environments pass.

To start developing Bloop first `create a fork`_, then clone and run the tests::

    git clone git@github.com:[YOU]/bloop.git
    cd bloop
    pip install tox -e .
    tox -e unit, docs

==========
Versioning
==========

.. _meta-versioning-public:

----------
Public API
----------

Bloop follows `Semantic Versioning 2.0.0`__ and a `draft appendix`__ for its :ref:`Public API <api-public>`.

The following are enforced:

* Backwards incompatible changes in major version only
* New features in minor version or higher
* Backwards compatible bug fixes in patch version or higher (see `appendix`_)

__ http://semver.org/spec/v2.0.0.html
__ appendix_
.. _appendix: https://gist.github.com/numberoverzero/c5d0fc6dea624533d004239a27e545ad

.. _versioning-internal:

------------
Internal API
------------

The :ref:`Internal API <api-internal>` is not versioned, and may make backwards incompatible changes at any time.
When a class or function is not explicitly documented as part on the public or internal api,
it is part of the internal api.  Still, please `open an issue`_ so it can be appropriately documented.

.. _open an issue: https://github.com/numberoverzero/bloop/issues/new
.. _create a pull request: https://github.com/numberoverzero/bloop/pull/new/master
.. _create a fork: https://github.com/numberoverzero/bloop/network

=======
License
=======

.. include:: ../../LICENSE
