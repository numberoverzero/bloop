#!/usr/bin/env bash

TOX_COMMAND="tox -e $TOXENV --"

if [ "$TOXENV" = "integ" ]; then
    TOX_COMMAND="$TOX_COMMAND --nonce=travis-$TRAVIS_BUILD_NUMBER-$TRAVIS_JOB_NUMBER"
fi

$TOX_COMMAND
