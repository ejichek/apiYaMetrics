#!/usr/bin/bash

export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

spark-submit \
--deploy-mode client \
/home/bdataadmin/airflow/test_script/pySpark_hits_basic_orc.py