#!/bin/bash

scp -P 2222 sql/db.hql root@localhost:/root/
ssh -o ServerAliveInterval=60 -p 2222 root@localhost 'hive -f db.hql'