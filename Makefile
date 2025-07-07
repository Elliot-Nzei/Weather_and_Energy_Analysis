
# Makefile for the Weather and Energy Analysis project

# Phony targets prevent conflicts with files of the same name
.PHONY: install run backfill

# Default target
all: install run

# Install Python dependencies
install:
	@echo "Installing dependencies..."
	python -m pip install -r requirements.txt

# Run the main data pipeline
run:
	@echo "Running the data pipeline..."
	python src/pipeline.py

# Run the backfill script for historical data
backfill:
	@echo "Backfilling historical data..."
	python backfill_historical.py

