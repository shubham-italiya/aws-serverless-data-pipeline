.PHONY: setup samples demo test lint glue-package

setup:          ## install the package and dev tools
	pip install -e ".[dev]"

samples:        ## real daily CSVs (Sep-Oct 2011) + 7 deliberately broken files in sample_data/
	python scripts/make_batches.py

demo:           ## run every sample file through the pipeline on a fake AWS account; writes reports/
	python scripts/demo.py

test:
	pytest -q

lint:
	ruff check .
	cfn-lint infra/template.yaml

glue-package:   ## the wheel the Glue job loads with --extra-py-files
	pip wheel . --no-deps -w dist
