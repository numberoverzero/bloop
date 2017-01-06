.PHONY: cov docs publish

cov:
	scripts/individual_coverage.sh

docs:
	cd docs && $(MAKE) html
	firefox docs/_build/html/index.html

publish:
	python setup.py sdist bdist_wheel
	twine upload dist/*
	rm -fr build dist .egg bloop.egg-info
