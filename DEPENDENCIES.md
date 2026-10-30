# Dependencies and Environment Setup

## Overview

This project uses Python 3.12 and several libraries for data processing, database connectivity, web scraping, image processing, statistical analysis, and machine learning.

Dependencies are separated into two files:

- **`requirements.txt`** – Core Python packages required to run the project.
- **`requirements-dev.txt`** – Additional tools used during development, including Jupyter Notebook and IPython kernel support.

## Environment Setup

This project uses Conda to manage its Python environment.

Create and activate the environment:

```powershell
conda create -n ALBUM_PROJ python=3.12 -y
conda activate ALBUM_PROJ
```

Install the core dependencies:

```powershell
python -m pip install -r requirements.txt
```

To install the additional development tools:

```powershell
python -m pip install -r requirements-dev.txt
```

## Core Dependencies

The project's core dependencies support the following areas:

| Area | Purpose |
|---|---|
| Data Handling | Working with structured data and numerical operations |
| Database Connectivity | Connecting to and querying PostgreSQL |
| Web Scraping | Retrieving album metadata and related information |
| Image Processing | Working with album artwork |
| Data Analysis | Statistical analysis and exploratory data analysis |
| Machine Learning | Developing and evaluating predictive models |
| Visualization | Creating charts and visual representations |

## Development Dependencies

Development dependencies provide additional tools for interactive Python development, particularly Jupyter Notebook and IPython kernel support.

These packages are not necessarily required to run the project's core Python scripts.

## Database Configuration

This project uses a local PostgreSQL database.

Database connection details and other environment-specific configuration are stored in a local `.env` file. This file is intentionally excluded from version control.

A `.env.example` file is provided as a template. Users must supply their own configuration values before running scripts that require database access.

## Important Notes

- Original album reviews and generated album artwork data are not included in this public repository.
- Users must configure their own local database and environment variables.
- Installing the Python dependencies does not automatically create or populate the PostgreSQL database.
- Package versions may change over time as dependencies are updated.