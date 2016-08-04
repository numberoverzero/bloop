.PHONY: cov docs

cov:
	scripts/individual_coverage.sh

docs:
	cd docs && $(MAKE) html
	firefox docs/_build/html/index.html
