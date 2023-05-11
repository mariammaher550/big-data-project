#!/bin/bash

#scp -P 2222 scripts/model.py root@localhost:/root/
ssh -p 2222 root@localhost 'spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packages org.apache.spark:spark-avro_2.12:3.0.3 model.py '
scp -P 2222 root@localhost:/project/output/als_predictions.csv/*.csv output/als_predictions.csv
scp -P 2222 root@localhost:/project/output/dt_predictions.csv/*.csv output/dt_predictions.csv
scp -P 2222 root@localhost:/project/output/als_rec_148744.csv/*.csv output/als_rec_148744.csv
scp -P 2222 root@localhost:/project/output/dt_rec_148744.csv/*.csv output/dt_rec_148744.csv