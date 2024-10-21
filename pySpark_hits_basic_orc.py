from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_hits_basic").getOrCreate()

hits_basic_schema = StructType([
	StructField('watchID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('dateTime', TimestampType(), True),
	StructField('pageViewID', StringType(), True),
	StructField('counterID', StringType(), True),
	StructField('clientID', StringType(), True),
	StructField('counterUserIDHash', StringType(), True),
	StructField('title', StringType(), True),
	StructField('goalsID', StringType(), True),
	StructField('URL', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(hits_basic_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('watchID').alias('watchID_1'),
	col('date').alias('date_1'),
	col('dateTime'),
	col('pageViewID').cast(LongType()),
	col('counterID').cast(LongType()),
	col('clientID'),
	col('counterUserIDHash'),
	col('title'),
	split(regexp_replace(col('goalsID'), r"(\]|\[)", ""), ",").alias('goalsID'),
	col('URL')
).filter("watchID_1 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_1") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_hits_basic")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),