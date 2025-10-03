.DEFAULT_GOAL := help
PYTHON_VERSION ?= py39

# Define the name of the virtual environment directory
VENV ?= .venv-dbt-impala

# Define the profile used by the dbt
PROFILE ?= dwx_endpoint

CHANGED_FILES := $(shell git ls-files --modified --other --exclude-standard)
CHANGED_FILES_IN_BRANCH := $(shell git diff --name-only $(shell git merge-base origin/master HEAD))

.PHONY: all install dev_setup test test_all_python_versions clean help
.PHONY: pre-commit pre-commit-in-branch pre-commit-all

all: dev_setup test  ## Default target for dev setup and run tests.

# Required python3 already installed in the system
$(VENV)/bin/activate:
	@\
	python3 -m venv $(VENV)

install: $(VENV)/bin/activate dev-requirements.txt	## Install all dependencies.
	@\
	$(VENV)/bin/python3 -m pip install --upgrade pip
	$(VENV)/bin/pip install -e . -r dev-requirements.txt

dev_setup: $(VENV)/bin/activate	 ## Install all dependencies and setup pre-commit.
	@\
	make install
	$(VENV)/bin/pre-commit install

test: ## Test specific version of python
	$(VENV)/bin/tox --recreate -e $(PYTHON_VERSION) -- $(TESTS) --profile $(PROFILE)

test_all_python_versions: ## Test all version of python
	$(VENV)/bin/tox --recreate -- $(TESTS) --profile $(PROFILE)

clean: 	## Cleanup and reset development environment.
	@echo 'cleaning virtual environment...'
	rm -rf $(VENV)
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -depth -delete
	@echo 'done.'

pre-commit:  ## check modified and added files (compared to last commit!) with pre-commit.
	$(VENV)/bin/pre-commit run --files $(CHANGED_FILES)

pre-commit-in-branch:  ## check changed since origin/master files with pre-commit.
	$(VENV)/bin/pre-commit run --files $(CHANGED_FILES_IN_BRANCH)

pre-commit-all:  ## Check all files in working directory with pre-commit.
	$(VENV)/bin/pre-commit run --all-files

help:  ## Show this help message.
	@echo 'usage: make [target] [VENV=.venv] [PROFILE=dwx_endpoint]'
	@echo
	@echo 'targets:'
	@grep -E '^[8+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'options:'
	@echo 'use VENV=<dir> to specify virtual enviroment directory'
	@echo 'use PROFILE=<profile> to use specific endpoint/warehouse'
