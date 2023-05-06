#!/bin/bash
#data/*.csv
scp -P 2222 scripts/load_psql.sh   sql/db.sql root@localhost:/root/

ssh -p 2222 root@localhost 'bash load_psql.sh'
