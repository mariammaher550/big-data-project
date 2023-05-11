#!/bin/bash

spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packages org.apache.spark:spark-avro_2.12:3.0.3 scripts/model.py 
cp output/als_predictions_dir/*.csv output/als_predictions.csv
cp output/als_rec_148744_dir/*.csv output/als_rec_148744.csv
cp output/dt_predictions_dir/*.csv output/dt_predictions.csv
cp output/dt_rec_148744_dir/*.csv output/dt_rec_148744.csv