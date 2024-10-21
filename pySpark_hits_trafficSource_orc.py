from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_hits_trafficSource").getOrCreate()

hits_trafficSource_schema = StructType([
	StructField('watchID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('referer', StringType(), True),
	StructField('UTMCampaign', StringType(), True),
	StructField('UTMContent', StringType(), True),
	StructField('UTMMedium', StringType(), True),
	StructField('UTMSource', StringType(), True),
	StructField('UTMTerm', StringType(), True),
	StructField('from', StringType(), True),
	StructField('hasGCLID', StringType(), True),
	StructField('GCLID', StringType(), True),
	StructField('lastTrafficSource', StringType(), True),
	StructField('lastSearchEngineRoot', StringType(), True),
	StructField('lastSearchEngine', StringType(), True),
	StructField('lastAdvEngine', StringType(), True),
	StructField('lastSocialNetwork', StringType(), True),
	StructField('lastSocialNetworkProfile', StringType(), True),
	StructField('recommendationSystem', StringType(), True),
	StructField('messenger', StringType(), True),
	StructField('browser', StringType(), True),
])

df1 = spark.read \
	.option("delimiter", "|") \
	.schema(hits_trafficSource_schema) \
	.csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('watchID').alias('watchID_2'),
	col('date').alias('date_2'),
	col('referer'),
	col('UTMCampaign'),
	col('UTMContent'),
	col('UTMMedium'),
	col('UTMSource'),
	col('UTMTerm'),
	col('from'),
	col('hasGCLID').cast(LongType()),
	col('GCLID'),
	col('lastTrafficSource'),
	col('lastSearchEngineRoot'),
	col('lastSearchEngine'),
	col('lastAdvEngine'),
	col('lastSocialNetwork'),
	col('lastSocialNetworkProfile'),
	col('recommendationSystem'),
	col('messenger'),
	col('browser')
).filter("watchID_2 is not null")

df2.show(3)

df2.write.format("orc") \
			.mode("append") \
			.partitionBy("date_2") \
			.option("maxRecordsPerFile", 150000) \
			.save("/user/azhalybin/airflow/test/orc_hits_TrafficSource")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),