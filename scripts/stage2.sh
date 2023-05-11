#!/bin/bash



hive -f sql/db.hql
bash scripts/to_csv.sh