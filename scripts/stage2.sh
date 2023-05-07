#!/bin/bash


scp -P 2222 sql/db.hql root@localhost:/root/
ssh -o ServerAliveInterval=60 -p 2222 root@localhost 'hive -f db.hql'
scp -P 2222 scripts/to_csv.sh root@localhost:/root/
ssh -p 2222 root@localhost 'bash to_csv.sh'
scp -P 2222  root@localhost:/project/output/*.csv output