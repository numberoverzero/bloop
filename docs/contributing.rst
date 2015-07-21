Contributing
============

Contributions welcome!  Please make sure ``tox`` passes before submitting a PR.
In addition to the test suite, this does require both flake8 and the docs build
to pass without warnings.

In general, pull requests that decrease coverage, do not include new unit
tests will not be accepted as-is.  New feature work is expected to include
additional documentation - especially for Public API changes.

Development
-----------
bloop uses `tox`, `pytest` and `flake8`.  To get everything set up with pyenv_:

.. code-block:: python

    git clone https://github.com/numberoverzero/bloop.git
    cd bloop
    pyenv virtualenv 3.4.3 bloop
    python setup.py develop
    tox

Documentation
-------------

Documentation improvements are especially appreciated.  If there's an area you
feel is lacking and will require more than a small change, `open an issue`_ to
discuss the problem - there are probably others that share your opinion and
have suggestions to improve the guide or docstring!

Because ``tox`` runs both the test suite and the doc build, it is still
required to pass for any documentation changes to be merged.

.. _pyenv: https://github.com/yyuu/pyenv
.. _open an issue: https://github.com/numberoverzero/bloop/issues/new
