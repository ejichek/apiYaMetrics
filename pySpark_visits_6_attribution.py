from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visit_6_attribution").getOrCreate()

visit_6_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('cross_device_lastAdvEngine', StringType(), True),
	StructField('cross_device_lastClickBannerGroupName', StringType(), True),
	StructField('cross_device_lastCurrencyID', StringType(), True),
	StructField('cross_device_lastDirectBannerGroup', StringType(), True),
	StructField('cross_device_lastDirectClickBanner', StringType(), True),
	StructField('cross_device_lastDirectClickBannerName', StringType(), True),
	StructField('cross_device_lastDirectClickOrder', StringType(), True),
	StructField('cross_device_lastDirectClickOrderName', StringType(), True),
	StructField('cross_device_lastDirectConditionType', StringType(), True),
	StructField('cross_device_lastDirectPhraseOrCond', StringType(), True),
	StructField('cross_device_lastDirectPlatform', StringType(), True),
	StructField('cross_device_lastDirectPlatformType', StringType(), True),
	StructField('cross_device_lastGCLID', StringType(), True),
	StructField('cross_device_lasthasGCLID', StringType(), True),
	StructField('cross_device_lastMessenger', StringType(), True),
	StructField('cross_device_lastRecommendationSystem', StringType(), True),
	StructField('cross_device_lastReferalSource', StringType(), True),
	StructField('cross_device_lastSearchEngine', StringType(), True),
	StructField('cross_device_lastSearchEngineRoot', StringType(), True),
	StructField('cross_device_lastSocialNetwork', StringType(), True),
	StructField('cross_device_lastSocialNetworkProfile', StringType(), True),
	StructField('cross_device_lastTrafficSource', StringType(), True),
	StructField('cross_device_lastUTMCampaign', StringType(), True),
	StructField('cross_device_lastUTMContent', StringType(), True),
	StructField('cross_device_lastUTMMedium', StringType(), True),
	StructField('cross_device_lastUTMSource', StringType(), True),
	StructField('cross_device_lastUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visit_6_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_6'),
	col('date').alias('date_6'),
	col('cross_device_lastAdvEngine'),
	col('cross_device_lastClickBannerGroupName'),
	col('cross_device_lastCurrencyID'),
	col('cross_device_lastDirectBannerGroup').cast(LongType()),
	col('cross_device_lastDirectClickBanner'),
	col('cross_device_lastDirectClickBannerName'),
	col('cross_device_lastDirectClickOrder').cast(LongType()),
	col('cross_device_lastDirectClickOrderName'),
	col('cross_device_lastDirectConditionType'),
	col('cross_device_lastDirectPhraseOrCond'),
	col('cross_device_lastDirectPlatform'),
	col('cross_device_lastDirectPlatformType'),
	col('cross_device_lastGCLID'),
	col('cross_device_lasthasGCLID').cast(LongType()),
	col('cross_device_lastMessenger'),
	col('cross_device_lastRecommendationSystem'),
	col('cross_device_lastReferalSource'),
	col('cross_device_lastSearchEngine'),
	col('cross_device_lastSearchEngineRoot'),
	col('cross_device_lastSocialNetwork'),
	col('cross_device_lastSocialNetworkProfile'),
	col('cross_device_lastTrafficSource'),
	col('cross_device_lastUTMCampaign'),
	col('cross_device_lastUTMContent'),
	col('cross_device_lastUTMMedium'),
	col('cross_device_lastUTMSource'),
	col('cross_device_lastUTMTerm')
).filter("visitID_6 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_6") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_6_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
