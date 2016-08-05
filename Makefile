.PHONY: cov docs

cov:
	scripts/individual_coverage.sh

docs:
	rm -rf docs/_build
	cd docs && $(MAKE) html
	firefox docs/_build/html/index.html
