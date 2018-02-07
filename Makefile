.PHONY: docs-clean
docs-clean:
	rm -rf docs/source/apidocs/*
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-build -a docs/source docs/build


.PHONY: test
test:
	python -m pytest -x --cov

.PHONY: dist
dist:
	rm -f MANIFEST
	rm -rf dist
	python setup.py bdist_wheel --universal
	# Upload with
	# twine upload dist/*
	# after checking version numbers again!

.PHONY: dist-clean
dist-clean:
	rm -rf ./dist
