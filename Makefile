# Makefile for the Weather and Energy Analysis project

# Phony targets prevent conflicts with files of the same name
.PHONY: install run analyze backfill backfill_weather backfill_energy clear_failed_fetches

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

# Run the analysis script
analyze:
	@echo "Running the analysis script..."
	python src/analysis.py

# Run the backfill script for historical data (both weather and energy)
backfill:
	@echo "Backfilling historical data (weather and energy)..."
	python backfill_historical.py

# Run the backfill script for weather data only
backfill_weather:
	@echo "Backfilling weather data only..."
	python backfill_historical.py --weather-only

# Run the backfill script for energy data only
backfill_energy:
	@echo "Backfilling energy data only..."
	python backfill_historical.py --energy-only

# Clear the failed fetches JSON file
clear_failed_fetches:
	@echo "Clearing failed fetches..."
	python backfill_historical.py --clear-failed-fetches

