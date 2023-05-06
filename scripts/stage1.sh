#!/bin/bash

scp -P 2222 scripts/load_psql.sh data/*.csv sql/db.sql root@localhost:/root/

ssh -p 2222 root@localhost 'bash load_psql.sh'
