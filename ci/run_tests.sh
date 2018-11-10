#! /usr/bin/env bash
set -euo pipefail

pipenv run black --fast --check --py36 *.py
pipenv run mypy *.py
pipenv run pytest
