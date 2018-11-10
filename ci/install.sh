#! /usr/bin/env bash
set -euo pipefail

pip install --user pipenv
pipenv check
pipenv sync --dev
