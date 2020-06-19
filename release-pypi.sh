#!/bin/bash

python3 setup.py sdist bdist_wheel || exit 1
python3 -m twine upload --repository pypi dist/*
