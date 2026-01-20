# nfl-injuries-mini-local (Windows Bash Makefile)
#
# This Makefile is written for Windows users running Bash (Git Bash / MSYS2 / WSL).
# It avoids “activate venv” by calling the venv’s python/pip directly.

# ----------------------------
# Basics / configuration
# ----------------------------

SHELL := bash
.ONESHELL:
.DEFAULT_GOAL := help

VENV := .venv
PY   := $(VENV)/Scripts/python
PIP  := $(VENV)/Scripts/pip

# ----------------------------
# Help (prints available targets)
# ----------------------------

.PHONY: help
help:
	@echo ""
	@echo "Targets:"
	@echo "  make venv        - create the virtual environment ($(VENV))"
	@echo "  make install     - install Python dependencies into the venv"
	@echo "  make run         - run the full pipeline (raw -> bronze -> silver -> gold)"
	@echo "  make clean-data  - delete generated CSV outputs"
	@echo "  make clean-venv  - delete the virtual environment folder"
	@echo "  make reset       - clean-data + rebuild venv + install + run"
	@echo ""

# ----------------------------
# Environment setup
# ----------------------------

.PHONY: venv
venv:
	python -m venv $(VENV)
	@echo "Created venv at $(VENV)"

.PHONY: install
install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Installed dependencies"

# ----------------------------
# Run pipeline
# ----------------------------

.PHONY: run
run: install
	$(PY) scripts/run_pipeline.py
	@echo "Pipeline complete"

# ----------------------------
# Cleanup
# ----------------------------

.PHONY: clean-data
clean-data:
	rm -f data/bronze/*.csv data/silver/*.csv data/gold/*.csv data/quarantine/*.csv
	@echo "Deleted generated CSV outputs"

.PHONY: clean-venv
clean-venv:
	rm -rf $(VENV)
	@echo "Deleted venv folder"

# ----------------------------
# Convenience
# ----------------------------

.PHONY: reset
reset: clean-data clean-venv
	$(MAKE) run
