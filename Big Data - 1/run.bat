@echo off
docker exec -i namenode hdfs dfs -test -d /input || \
docker exec -i namenode hdfs dfs -mkdir /input

docker exec -i namenode hdfs dfs -test -f /input/us_accidents_cleaned.csv || \
docker exec -i namenode hdfs dfs -put /data/us_accidents_cleaned.csv /input/
docker exec -i spark-master /spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --name big-data-app ^
  /opt/spark-apps/cluster-app.py ^
  City Temperature_F Distance_mi ^
  2016-01-01 2025-01-31
pause
