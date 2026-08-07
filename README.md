# uslci-moves

Generates openLCA datasets for on-road MOVES trucking and non-road equipment.

## Install

Requires [flcac-utils](https://github.com/FLCAC-admin/flcac-utils).

> pip install git+https://github.com/FLCAC-admin/flcac-utils.git

Run [processing_MOVES.py](processing_MOVES.py)

Run [processing_nonroad_MOVES.py](processing_nonroad_MOVES.py)

`processing_MOVES.py` writes two JSON-LD packages:

1. **On-road MHD freight**
2. **Refuse trucks**

## Datasets

| Datasts            | Version | flcac-utils | Release        |
|--------------------|---------|-------------|----------------|
| On-road Refuse     | v1.0.0  | v0.4.0      | -              |
| Non-road equipment | v1.0.0  | v0.3.0      | 2026 Q1, USLCI |
| On-road Trucking   | v1.0.0  | v0.1.0      | 2025 Q1, USLCI |
