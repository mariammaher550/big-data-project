#!/bin/bash
psql -U postgres -c 'DROP DATABASE IF EXISTS project;'
psql -U postgres -c'CREATE DATABASE project;'
psql -U postgres -d project -f db.sql
# psql -U postgres -c "select * from users;"
# ls
hdfs dfs -rm -r /project
sqoop import-all-tables \
    -Dmapreduce.job.user.classpath.first=true \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --warehouse-dir /project \
    --as-avrodatafile \
    --compression-codec=snappy \
    --outdir /project/avsc \
    --m 1
hdfs dfs -mkdir /project/avsc
hdfs dfs -put /project/*/*.avsc /project/avsc