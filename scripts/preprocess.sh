#!/bin/bash

echo "installing requirements"
pip install -r requirements.txt --ignore-installed --quiet
python scripts/preprocessing.py