#!/usr/bin/bash

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_1_Basic_Events/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "1_Basic_Events empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_1_Basic_Events/*'
	echo "del 1_Basic_Events"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_2_Ecommerce/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "2_Ecommerce empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_2_Ecommerce/*'
	echo "del 2_Ecommerce"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_3_Goals/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "3_Goals empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_3_Goals/*'
	echo "del 3_Goals"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_4_Device_Source/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "4_Device_Source empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_4_Device_Source/*'
	echo "del 4_Device_Source"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_5_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "5_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_5_Attribution/*'
	echo "del 5_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_6_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "6_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_6_Attribution/*'
	echo "del 6_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_7_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "7_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_7_Attribution/*'
	echo "del 7_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_8_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "8_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_8_Attribution/*'
	echo "del 8_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_9_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "9_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_9_Attribution/*'
	echo "del 9_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_10_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "10_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_10_Attribution/*'
	echo "del 10_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_11_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "11_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_11_Attribution/*'
	echo "del 11_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_12_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "12_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_12_Attribution/*'
	echo "del 12_Attribution"
fi

isEmpty=$(hdfs dfs -count /user/azhalybin/airflow/test/orc_visits_13_Attribution/ | awk '{print $2}')
if [[ $isEmpty -eq 0 ]];then
    echo "13_Attribution empety"
else
    hdfs dfs -rm -r '/user/azhalybin/airflow/test/orc_visits_13_Attribution/*'
	echo "del 13_Attribution"
fi