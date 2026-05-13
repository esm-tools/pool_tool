# Contributing to ptool

Thank you for considering contributing to ptool! This document describes how to get started.

## Development setup

1. Fork the repository and clone your fork:

   ```shell
   git clone https://github.com/<your-username>/pool_tool.git
   cd pool_tool
   ```

2. Create the conda environment and install the package in editable mode:

   ```shell
   conda env create -f environment.yaml
   conda activate ptool
   pip install -e ".[dev]"
   ```

3. Verify the setup by running the test suite:

   ```shell
   pytest tests/ -v
   ```

## Workflow

- Create a feature branch from `main`:

  ```shell
  git checkout -b my-feature
  ```

- Make your changes, add tests, and ensure all tests pass before opening a PR.
- Write clear, descriptive commit messages.
- Open a pull request against `main` and fill in the PR template.

## Running tests

```shell
pytest tests/ -v
```

## Reporting bugs

Please use the [bug report template](https://github.com/esm-tools/pool_tool/issues/new?template=bug_report.yml) when filing issues.

## Code of conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating you agree to abide by its terms.
