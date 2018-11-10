#! /usr/bin/env bash
set -eo pipefail

if [[ "$VIRTUAL_ENV" == "" ]]; then
    pip install pipenv
else
    pip install --user pipenv
fi

pipenv check
pipenv sync --dev
