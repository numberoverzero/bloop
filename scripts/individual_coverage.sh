#!/usr/bin/env bash

# clean up existing coverage since every call below appends
rm -f .coverage
for filename in bloop/*.py; do
    # strip directory
    module=$(basename $filename)
    # skip root import file
    [[ "$module" == "__init__.py" ]] && continue
    echo "Collecting ${module%.*}..."
    coverage run --append --branch --source=bloop/$module -m py.test tests/unit/test_$module &> /dev/null 2>&1
done
coverage report -m
