#!/usr/bin/env bash
TOX_COMMAND="tox -e $TOXENV --"

if [ "$TOXENV" = "integ" ]; then
    if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
        TOX_COMMAND="echo Pull requests cannot run integration tests"
    else
        TOX_COMMAND="$TOX_COMMAND --nonce=-travis-$TRAVIS_JOB_NUMBER"
    fi
fi

${TOX_COMMAND}
