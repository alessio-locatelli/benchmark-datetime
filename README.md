# benchmark-datetime

## How to setup

1. Install [Poetry](https://python-poetry.org/).
2. Activate the virtual environment and install dependencies: `poetry shell && poetry install`.

Some packages (for example, `udatetime`) may require additional dependencies for building from source.
Read the documentation for specific packages if the installation fails.

## How to run

```sh
pytest benchmark_datetime/benchmark.py --benchmark-group-by=func
```
