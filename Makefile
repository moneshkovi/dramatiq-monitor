.PHONY: install dev stop test seed build clean

CONDA_ENV ?= dramatiq
# Resolve the env's interpreter by absolute path rather than `conda run -n ...`
# (which relies on PATH-based command lookup): if your shell profile already
# puts a different conda env's bin/ ahead of the one conda run prepends —
# common if you `conda activate` a default env in .bashrc/.zshrc — `conda run`
# silently executes the WRONG environment's python instead of failing loudly.
# Absolute paths sidestep that regardless of shell PATH ordering.
CONDA_BASE := $(shell conda info --base 2>/dev/null)
PYTHON := $(CONDA_BASE)/envs/$(CONDA_ENV)/bin/python

install:  ## Editable-install the package + dev dependencies into the conda env
	$(PYTHON) -m pip install -e ".[dev]"

dev:  ## Start Docker Redis + seed demo data + serve on :8321 (actions enabled)
	./scripts/dev.sh

stop:  ## Tear down the demo Redis container + dev server started by `make dev`
	./scripts/stop.sh

test:  ## Run the test suite (fakeredis, no real Redis needed)
	$(PYTHON) -m pytest -q

seed:  ## Seed demo data into a Redis you already have running (redis://127.0.0.1:6379 by default)
	$(PYTHON) scripts/seed_demo.py

build:  ## Build the wheel/sdist (sanity check that templates/static/fonts are packaged)
	$(PYTHON) -m build

clean:  ## Remove build artifacts and caches
	rm -rf dist build src/*.egg-info .pytest_cache
	find . -name __pycache__ -type d -not -path "./.git/*" -exec rm -rf {} +
