#!/usr/bin/env bash

TOX_COMMAND="tox -e $TOXENV --"

if [ "$TOXENV" = "integ" ]; then
    TOX_COMMAND = "$TOX_COMMAND --nonce=travis-$TRAVIS_BUILD_ID-$TRAVIS_BUILD_NUMBER"
fi

$TOX_COMMAND
