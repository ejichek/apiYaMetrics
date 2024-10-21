#!/usr/bin/bash

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_hits_basic/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "orc_hits_basic empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_hits_basic/*'
	echo "del orc_hits_basic"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_hits_TrafficSource/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "orc_hits_TrafficSource empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_hits_TrafficSource/*'
	echo "del orc_hits_TrafficSource"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_hits_Ecommerce_EventParams_EventType/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "orc_hits_Ecommerce_EventParams_EventType empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_hits_Ecommerce_EventParams_EventType/*'
	echo "del orc_hits_Ecommerce_EventParams_EventType"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_hits_Device/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "orc_hits_Device empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_hits_Device/*'
	echo "del orc_hits_Device"
fi