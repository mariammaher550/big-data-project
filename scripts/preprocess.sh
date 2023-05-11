#!/bin/bash

#echo "installing requirements locally"
#pip install -r requirements.txt --quiet
#scp -P 2222 requirements.txt root@localhost:/root/
echo "installing requirements in hdfs"
pip install -r requirements.txt --ignore-installed
python scripts/preprocessing.py