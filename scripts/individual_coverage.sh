#!/usr/bin/env bash

# Run tests for one module
cover_file () {
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
        cover_file
    done
}

# clean up existing coverage since every cover_file appends to existing
rm -f .coverage

if [ -z "$1" ]; then
    # No module specified, run all
    echo "Unit tests for all modules"
    cover_all
else
    # Run specified module
    echo "Unit tests for single module"
    MODULE="$1".py
    cover_file
fi

coverage report -m
