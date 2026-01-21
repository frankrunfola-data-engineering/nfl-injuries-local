# nfl-injuries-mini-local (Windows Bash Makefile)
#
# Purpose:
# - One-command local setup + run for a tiny medallion-style pipeline.
#
# Designed for:
# - Windows users running a Bash shell (Git Bash / MSYS2 / WSL).
#
# Key idea:
# - No manual "activate venv" step.
# - We call the venv’s python directly.
# - IMPORTANT (Windows): use `python -m pip` instead of `pip.exe` so pip can upgrade itself.

SHELL := bash
.ONESHELL:
.DEFAULT_GOAL := help

# ----------------------------
# Basics / configuration
# ----------------------------

# Virtual environment folder name.
VENV := .venv

# Windows venv executables live under Scripts/
# (On WSL/Linux, these would be .venv/bin/python and .venv/bin/pip)
PY      := $(VENV)/Scripts/python
PIP_CMD := $(PY) -m pip

# Reusable banner line for nicer console output.
BANNER_LINE := "********************************************************"

# ----------------------------
# Help (prints available targets)
# ----------------------------

.PHONY: help
help:
	@echo ""
	@echo "Targets:"
	@echo ""
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
	@echo ""
	@echo $(BANNER_LINE)
	@echo "Creating virtual environment at $(VENV)..."
	@echo $(BANNER_LINE)

	# If the venv already exists, skip creation to avoid nuking user installs.
	@if [ -d "$(VENV)" ]; then \
		echo ""; \
		echo "Venv already exists at $(VENV)."; \
		echo "Skipping create."; \
		echo ""; \
		exit 0; \
	fi

	# Create the virtual environment using the system python.
	python -m venv $(VENV)

	@echo ""
	@echo $(BANNER_LINE)
	@echo "Created venv at $(VENV)"
	@echo $(BANNER_LINE)
	@echo ""

.PHONY: install
install: venv
	@echo ""
	@echo $(BANNER_LINE)
	@echo "Upgrading pip and installing dependencies..."
	@echo $(BANNER_LINE)
	@echo ""

	# IMPORTANT (Windows):
	# Use `python -m pip` so pip can upgrade itself safely (calling pip.exe can fail).
	$(PIP_CMD) install --upgrade pip
	$(PIP_CMD) install -r requirements.txt

	@echo "Dependencies installed."
	@echo ""

# ----------------------------
# Run pipeline
# ----------------------------

.PHONY: run
run: install
	@echo ""
	@echo $(BANNER_LINE)
	@echo "Running full pipeline: raw -> bronze -> silver -> gold"
	@echo $(BANNER_LINE)
	@echo ""

	# Run the CLI entrypoint script using the venv python.
	$(PY) scripts/run_pipeline.py

	@echo $(BANNER_LINE)
	@echo "Pipeline complete."
	@echo $(BANNER_LINE)
	@echo ""

# ----------------------------
# Cleanup
# ----------------------------

.PHONY: clean-data
clean-data:
	@echo ""
	@echo $(BANNER_LINE)
	@echo "Deleting generated CSV outputs..."
	@echo $(BANNER_LINE)
	@echo ""

	# Delete generated outputs but keep data/raw inputs intact.
	rm -f data/bronze/*.csv data/silver/*.csv data/gold/*.csv data/quarantine/*.csv

	@echo "Deleted generated CSV outputs."
	@echo ""

.PHONY: clean-venv
clean-venv:
	@echo ""
	@echo $(BANNER_LINE)
	@echo "Deleting venv folder: $(VENV)"
	@echo $(BANNER_LINE)
	@echo ""

	# Remove the entire virtual environment folder.
	rm -rf $(VENV)

	@echo "Deleted venv folder."
	@echo ""

# ----------------------------
# Convenience
# ----------------------------

.PHONY: reset
reset: clean-data clean-venv
	@echo ""
	@echo "Resetting environment..."
	@echo ""

	# After cleanup, rebuild + install + run.
	$(MAKE) run
