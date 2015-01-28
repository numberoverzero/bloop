# bloop 0.1.0

[![Build Status]
(https://travis-ci.org/numberoverzero/bloop.svg?branch=master)]
(https://travis-ci.org/numberoverzero/bloop)[![Coverage Status]
(https://coveralls.io/repos/numberoverzero/bloop/badge.png?branch=master)]
(https://coveralls.io/r/numberoverzero/bloop?branch=master)

Downloads https://pypi.python.org/pypi/bloop

Source https://github.com/numberoverzero/bloop

ORM for DynamoDB

# Installation

`pip install bloop`

# Getting Started

Oh god how did this get in here I am not good with computers

# Versioning

* bloop follows semver for its **public** API.

  * You should not rely on the internal api staying the same between minor versions.
  * Over time, private apis may be raised to become public.  The reverse will never occur.

# Contributing
Contributions welcome!  Please make sure `tox` passes (including flake8) before submitting a PR.

### Development
bloop uses `tox`, `pytest` and `flake8`.  To get everything set up:

```
# RECOMMENDED: create a virtualenv with:
#     mkvirtualenv bloop
git clone https://github.com/numberoverzero/bloop.git
pip install tox
tox
```

### TODO

* Engine
* Table
* Column
* Type
