PYTHON ?= python3
DB ?= luna_mlb.sqlite
BUNDLE ?= data/fixtures/bundles/sample_boxscore_bundle.json

.PHONY: install lint test smoke-ingest smoke-derive smoke-dashboard

install:
	$(PYTHON) -m pip install -r requirements.txt

lint:
	PYTHONPATH=src $(PYTHON) -m ruff check src tests scripts

test:
	PYTHONPATH=src $(PYTHON) -m pytest

smoke-ingest:
	PYTHONPATH=src $(PYTHON) scripts/run_ingest.py --bundle $(BUNDLE) --db $(DB)

smoke-derive:
	PYTHONPATH=src $(PYTHON) scripts/run_derivations.py --db $(DB)

smoke-dashboard:
	PYTHONPATH=src $(PYTHON) scripts/smoke_run.py --db $(DB)

refresh-public-assets:
	PYTHONPATH=src $(PYTHON) scripts/refresh_public_assets.py --bundle $(BUNDLE) --output-dir docs/proof
