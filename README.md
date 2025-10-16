# Alerting MVP

## Overview
This repository contains an MVP implementation plan for a crypto alerting platform covering Binance/DEX token monitoring, multi-level price alerts, volume/trend rules, notifications, and a Streamlit web UI.

## Requirements
- Python 3.10+
- pip

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use `.venv\\Scripts\\activate`
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env  # Fill in secrets as needed
```

## Running
The project entrypoint is `run.py`. The script currently offers two modes:

```bash
python run.py --help
python run.py --once
python run.py --loop
```

`--once` runs a single iteration of the pipeline, while `--loop` will start the long-running service (currently stubbed).

## Project Structure
```
connectors/            # Market data adapters
aggregator/            # Timeframe aggregation
indicators/            # Technical indicators
rules/                 # Rule engines
alerts/                # Notification channels and router
ui/                    # Streamlit UI
agent/                 # Local sound agent
storage/               # SQLite management and migrations
backtest/              # Backtesting utilities
demo/                  # Sample data loaders
config.yaml            # Main configuration
dotenv example         # Environment variables sample
requirements.txt       # Python dependencies
run.py                 # Main entrypoint
```
