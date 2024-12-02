from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_7_attribution").getOrCreate()

visits_7_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('cross_device_last_significantAdvEngine', StringType(), True),
	StructField('cross_device_last_significantClickBannerGroupName', StringType(), True),
	StructField('cross_device_last_significantCurrencyID', StringType(), True),
	StructField('cross_device_last_significantDirectBannerGroup', StringType(), True),
	StructField('cross_device_last_significantDirectClickBanner', StringType(), True),
	StructField('cross_device_last_significantDirectClickBannerName', StringType(), True),
	StructField('cross_device_last_significantDirectClickOrder', StringType(), True),
	StructField('cross_device_last_significantDirectClickOrderName', StringType(), True),
	StructField('cross_device_last_significantDirectConditionType', StringType(), True),
	StructField('cross_device_last_significantDirectPhraseOrCond', StringType(), True),
	StructField('cross_device_last_significantDirectPlatform', StringType(), True),
	StructField('cross_device_last_significantDirectPlatformType', StringType(), True),
	StructField('cross_device_last_significantGCLID', StringType(), True),
	StructField('cross_device_last_significanthasGCLID', StringType(), True),
	StructField('cross_device_last_significantMessenger', StringType(), True),
	StructField('cross_device_last_significantRecommendationSystem', StringType(), True),
	StructField('cross_device_last_significantReferalSource', StringType(), True),
	StructField('cross_device_last_significantSearchEngine', StringType(), True),
	StructField('cross_device_last_significantSearchEngineRoot', StringType(), True),
	StructField('cross_device_last_significantSocialNetwork', StringType(), True),
	StructField('cross_device_last_significantSocialNetworkProfile', StringType(), True),
	StructField('cross_device_last_significantTrafficSource', StringType(), True),
	StructField('cross_device_last_significantUTMCampaign', StringType(), True),
	StructField('cross_device_last_significantUTMContent', StringType(), True),
	StructField('cross_device_last_significantUTMMedium', StringType(), True),
	StructField('cross_device_last_significantUTMSource', StringType(), True),
	StructField('cross_device_last_significantUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visits_7_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_7'),
	col('date').alias('date_7'),
	col('cross_device_last_significantAdvEngine'),
	col('cross_device_last_significantClickBannerGroupName'),
	col('cross_device_last_significantCurrencyID'),
	col('cross_device_last_significantDirectBannerGroup').cast(LongType()),
	col('cross_device_last_significantDirectClickBanner'),
	col('cross_device_last_significantDirectClickBannerName'),
	col('cross_device_last_significantDirectClickOrder').cast(LongType()),
	col('cross_device_last_significantDirectClickOrderName'),
	col('cross_device_last_significantDirectConditionType'),
	col('cross_device_last_significantDirectPhraseOrCond'),
	col('cross_device_last_significantDirectPlatform'),
	col('cross_device_last_significantDirectPlatformType'),
	col('cross_device_last_significantGCLID'),
	col('cross_device_last_significanthasGCLID').cast(LongType()),
	col('cross_device_last_significantMessenger'),
	col('cross_device_last_significantRecommendationSystem'),
	col('cross_device_last_significantReferalSource'),
	col('cross_device_last_significantSearchEngine'),
	col('cross_device_last_significantSearchEngineRoot'),
	col('cross_device_last_significantSocialNetwork'),
	col('cross_device_last_significantSocialNetworkProfile'),
	col('cross_device_last_significantTrafficSource'),
	col('cross_device_last_significantUTMCampaign'),
	col('cross_device_last_significantUTMContent'),
	col('cross_device_last_significantUTMMedium'),
	col('cross_device_last_significantUTMSource'),
	col('cross_device_last_significantUTMTerm')
).filter("visitID_7 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_7") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_7_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
