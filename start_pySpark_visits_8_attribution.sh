#!/usr/bin/bash

export PYSPARK_PYTHON="/opt/anaconda3/bin/python"

spark-submit \
--deploy-mode client \
/home/bdataadmin/airflow/test_script/pySpark_visits_8_attribution.py