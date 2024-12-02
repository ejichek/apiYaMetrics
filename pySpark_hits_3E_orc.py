from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_hits_Ecommerce_EventParams_EventType").getOrCreate()

hits_3E_schema = StructType([
	StructField('watchID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('link', StringType(), True),
	StructField('download', StringType(), True),
	StructField('notBounce', StringType(), True),
	StructField('artificial', StringType(), True),
	StructField('ecommerce', StringType(), True),
	StructField('params', StringType(), True),
	StructField('parsedParamsKey1', StringType(), True),
	StructField('parsedParamsKey2', StringType(), True),
	StructField('parsedParamsKey3', StringType(), True),
	StructField('parsedParamsKey4', StringType(), True),
	StructField('parsedParamsKey5', StringType(), True),
	StructField('parsedParamsKey6', StringType(), True),
	StructField('parsedParamsKey7', StringType(), True),
	StructField('parsedParamsKey8', StringType(), True),
	StructField('parsedParamsKey9', StringType(), True),
	StructField('parsedParamsKey10', StringType(), True),
	StructField('httpError', StringType(), True),
	StructField('shareService', StringType(), True),
	StructField('shareURL', StringType(), True),
	StructField('shareTitle', StringType(), True)
])

df1 = spark.read \
	.option("delimiter", "\t") \
	.schema(hits_3E_schema) \
	.csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('watchID').alias('watchID_3'),
	col('date').alias('date_3'),
	col('link').cast(LongType()),
	col('download').cast(LongType()),
	col('notBounce').cast(LongType()),
	col('artificial').cast(LongType()),
	col('ecommerce'),
	col('params'),
	split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
	split(regexp_replace(col('parsedParamsKey2'), r"(\]|\[)", ""), ",").alias('parsedParamsKey2'),
	split(regexp_replace(col('parsedParamsKey3'), r"(\]|\[)", ""), ",").alias('parsedParamsKey3'),
	split(regexp_replace(col('parsedParamsKey4'), r"(\]|\[)", ""), ",").alias('parsedParamsKey4'),
	split(regexp_replace(col('parsedParamsKey5'), r"(\]|\[)", ""), ",").alias('parsedParamsKey5'),
	split(regexp_replace(col('parsedParamsKey6'), r"(\]|\[)", ""), ",").alias('parsedParamsKey6'),
	split(regexp_replace(col('parsedParamsKey7'), r"(\]|\[)", ""), ",").alias('parsedParamsKey7'),
	split(regexp_replace(col('parsedParamsKey8'), r"(\]|\[)", ""), ",").alias('parsedParamsKey8'),
	split(regexp_replace(col('parsedParamsKey9'), r"(\]|\[)", ""), ",").alias('parsedParamsKey9'),
	split(regexp_replace(col('parsedParamsKey10'), r"(\]|\[)", ""), ",").alias('parsedParamsKey10'),
	col('httpError'),
	col('shareService'),
	col('shareURL'),
	col('shareTitle')
).filter("watchID_3 is not null")

df2.show(3)

df2.write.format("orc") \
			.mode("append") \
			.partitionBy("date_3") \
			.option("maxRecordsPerFile", 150000) \
			.save("/user/azhalybin/airflow/test/orc_hits_Ecommerce_EventParams_EventType")

#	split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
