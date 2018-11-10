#! /usr/bin/env bash
set -eo pipefail

if [[ "$VIRTUAL_ENV" == "" ]]; then
    pip install --user pipenv
else
    pip install pipenv
fi

pipenv check
pipenv sync --dev
