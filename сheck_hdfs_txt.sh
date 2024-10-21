#!/usr/bin/bash

hdfs dfs -test -e /user/azhalybin/airflow/test/txt/final_txt.txt 

if [ $? -eq 0 ]
then
	hdfs dfs -rm /user/azhalybin/airflow/test/txt/final_txt.txt
	echo "del final_txt.txt"
else
	echo "final_txt.txt file doesn't exist in the hdfs directory"
fi
