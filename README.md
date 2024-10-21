# apiYaMetrics

ETL процесс на базе AirFlow, Python и PySpark по выгрузке visits и hits по частям (используя бесплатное хранилище на стороне Яндекс Метрики размером 10Гб). 
Источник данных - API Яндекс Метки, получатель - HDFS, файлы хранятся в orc и партиционируются по полю date с "шагом" в 1 день.  
