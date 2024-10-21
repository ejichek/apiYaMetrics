from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_device_source").getOrCreate()

visits_device_source_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('from', StringType(), True),
	StructField('referer', StringType(), True),
	StructField('browser', StringType(), True),
	StructField('browserCountry', StringType(), True),
	StructField('browserEngine', StringType(), True),
	StructField('browserEngineVersion1', StringType(), True),
	StructField('browserEngineVersion2', StringType(), True),
	StructField('browserEngineVersion3', StringType(), True),
	StructField('browserEngineVersion4', StringType(), True),
	StructField('browserLanguage', StringType(), True),
	StructField('browserMajorVersion', StringType(), True),
	StructField('browserMinorVersion', StringType(), True),
	StructField('clientTimeZone', StringType(), True),
	StructField('cookieEnabled', StringType(), True),
	StructField('deviceCategory', StringType(), True),
	StructField('javascriptEnabled', StringType(), True),
	StructField('mobilePhone', StringType(), True),
	StructField('mobilePhoneModel', StringType(), True),
	StructField('operatingSystem', StringType(), True),
	StructField('operatingSystemRoot', StringType(), True),
	StructField('physicalScreenHeight', StringType(), True),
	StructField('physicalScreenWidth', StringType(), True),
	StructField('screenColors', StringType(), True),
	StructField('screenFormat', StringType(), True),
	StructField('screenHeight', StringType(), True),
	StructField('screenOrientation', StringType(), True),
	StructField('screenWidth', StringType(), True),
	StructField('windowClientHeight', StringType(), True),
	StructField('windowClientWidth', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(visits_device_source_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_4'),
	col('date').alias('date_4'),
	col('from'),
	col('referer'),
	col('browser'),
	col('browserCountry'),
	col('browserEngine'),
	col('browserEngineVersion1').cast(LongType()),
	col('browserEngineVersion2').cast(LongType()),
	col('browserEngineVersion3').cast(LongType()),
	col('browserEngineVersion4').cast(LongType()),
	col('browserLanguage'),
	col('browserMajorVersion').cast(LongType()),
	col('browserMinorVersion').cast(LongType()),
	col('clientTimeZone').cast(LongType()),
	col('cookieEnabled').cast(LongType()),
	col('deviceCategory'),
	col('javascriptEnabled').cast(LongType()),
	col('mobilePhone'),
	col('mobilePhoneModel'),
	col('operatingSystem'),
	col('operatingSystemRoot'),
	col('physicalScreenHeight').cast(LongType()),
	col('physicalScreenWidth').cast(LongType()),
	col('screenColors').cast(LongType()),
	col('screenFormat'),
	col('screenHeight').cast(LongType()),
	col('screenOrientation'),
	col('screenWidth').cast(LongType()),
	col('windowClientHeight').cast(LongType()),
	col('windowClientWidth').cast(LongType())
).filter("visitID_4 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_4") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_4_Device_Source")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),