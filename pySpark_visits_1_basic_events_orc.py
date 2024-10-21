from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visit_basic_events").getOrCreate()

visits_basic_events_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('parsedParamsKey1', StringType(), True),
	StructField('parsedParamsKey10', StringType(), True),
	StructField('parsedParamsKey2', StringType(), True),
	StructField('parsedParamsKey3', StringType(), True),
	StructField('parsedParamsKey4', StringType(), True),
	StructField('parsedParamsKey5', StringType(), True),
	StructField('parsedParamsKey6', StringType(), True),
	StructField('parsedParamsKey7', StringType(), True),
	StructField('parsedParamsKey8', StringType(), True),
	StructField('parsedParamsKey9', StringType(), True),
	StructField('goalsCurrency', StringType(), True),
	StructField('goalsDateTime', StringType(), True),
	StructField('goalsID', StringType(), True),
	StructField('goalsOrder', StringType(), True),
	StructField('goalsPrice', StringType(), True),
	StructField('goalsSerialNumber', StringType(), True),
	StructField('bounce', StringType(), True),
	StructField('clientID', StringType(), True),
	StructField('counterID', StringType(), True),
	StructField('counterUserIDHash', StringType(), True),
	StructField('dateTime', StringType(), True),
	StructField('dateTimeUTC', StringType(), True),
	StructField('endURL', StringType(), True),
	StructField('ipAddress', StringType(), True),
	StructField('isNewUser', StringType(), True),
	StructField('pageViews', StringType(), True),
	StructField('regionCity', StringType(), True),
	StructField('regionCityID', StringType(), True),
	StructField('regionCountry', StringType(), True),
	StructField('regionCountryID', StringType(), True),
	StructField('startURL', StringType(), True),
	StructField('visitDuration', StringType(), True),
	StructField('watchIDs', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(visits_basic_events_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_1'),
	col('date').alias('date_1'),
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
	split(regexp_replace(col('goalsCurrency'), r"(\]|\[)", ""), ",").alias('goalsCurrency'),
	split(regexp_replace(col('goalsDateTime'), r"(\]|\[)", ""), ",").alias('goalsDateTime'),
	split(regexp_replace(col('goalsID'), r"(\]|\[)", ""), ",").alias('goalsID'),
	split(regexp_replace(col('goalsOrder'), r"(\]|\[)", ""), ",").alias('goalsOrder'),
	split(regexp_replace(col('goalsPrice'), r"(\]|\[)", ""), ",").alias('goalsPrice'),
	split(regexp_replace(col('goalsSerialNumber'), r"(\]|\[)", ""), ",").alias('goalsSerialNumber'),
	col('bounce').cast(LongType()),
	col('clientID'),
	col('counterID').cast(LongType()),
	col('counterUserIDHash'),
	col('dateTime').cast(TimestampType()),
	col('dateTimeUTC').cast(TimestampType()),
	col('endURL'),
	col('ipAddress'),
	col('isNewUser').cast(LongType()),
	col('pageViews').cast(LongType()),
	col('regionCity'),
	col('regionCityID').cast(LongType()),
	col('regionCountry'),
	col('regionCountryID').cast(LongType()),
	col('startURL'),
	col('visitDuration').cast(LongType()),
	split(regexp_replace(col('watchIDs'), r"(\]|\[)", ""), ",").alias('watchIDs')
).filter("visitID_1 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_1") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_1_Basic_Events")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),