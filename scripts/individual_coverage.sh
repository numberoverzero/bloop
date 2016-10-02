#!/usr/bin/env bash

# Output test status when running a single module's tests
cover_file () {
    coverage run --branch --source=bloop/$MODULE -m py.test tests/unit/test_$MODULE
}

# Just collecting coverage, suppress test output
cover_silent () {
    coverage run --append --branch --source=bloop/$MODULE -m py.test tests/unit/test_$MODULE &> /dev/null 2>&1
}
# Discover all modules and run tests against each
cover_all () {
    for filename in bloop/*.py; do
        # strip directory
        MODULE=$(basename $filename)
        # skip root import file
        [[ "$MODULE" == "__init__.py" ]] && continue
        echo "Collecting ${MODULE%.*}..."
        cover_silent
    done
}

# clean up existing coverage since every cover_file appends to existing
rm -f .coverage

if [ -z "$1" ]; then
    # No module specified, run all
    echo "Unit tests for all modules, suppressing test output"
    cover_all
else
    # Run specified module
    echo "Unit tests for single module"
    MODULE="$1".py
    cover_file
fi

coverage report -m
