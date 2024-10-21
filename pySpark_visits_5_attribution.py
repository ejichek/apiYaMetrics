from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visit_attribution_5").getOrCreate()

visit_attribution_5_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('cross_device_firstAdvEngine', StringType(), True),
	StructField('cross_device_firstClickBannerGroupName', StringType(), True),
	StructField('cross_device_firstCurrencyID', StringType(), True),
	StructField('cross_device_firstDirectBannerGroup', StringType(), True),
	StructField('cross_device_firstDirectClickBanner', StringType(), True),
	StructField('cross_device_firstDirectClickBannerName', StringType(), True),
	StructField('cross_device_firstDirectClickOrder', StringType(), True),
	StructField('cross_device_firstDirectClickOrderName', StringType(), True),
	StructField('cross_device_firstDirectConditionType', StringType(), True),
	StructField('cross_device_firstDirectPhraseOrCond', StringType(), True),
	StructField('cross_device_firstDirectPlatform', StringType(), True),
	StructField('cross_device_firstDirectPlatformType', StringType(), True),
	StructField('cross_device_firstGCLID', StringType(), True),
	StructField('cross_device_firsthasGCLID', StringType(), True),
	StructField('cross_device_firstMessenger', StringType(), True),
	StructField('cross_device_firstRecommendationSystem', StringType(), True),
	StructField('cross_device_firstReferalSource', StringType(), True),
	StructField('cross_device_firstSearchEngine', StringType(), True),
	StructField('cross_device_firstSearchEngineRoot', StringType(), True),
	StructField('cross_device_firstSocialNetwork', StringType(), True),
	StructField('cross_device_firstSocialNetworkProfile', StringType(), True),
	StructField('cross_device_firstTrafficSource', StringType(), True),
	StructField('cross_device_firstUTMCampaign', StringType(), True),
	StructField('cross_device_firstUTMContent', StringType(), True),
	StructField('cross_device_firstUTMMedium', StringType(), True),
	StructField('cross_device_firstUTMSource', StringType(), True),
	StructField('cross_device_firstUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(visit_attribution_5_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_5'),
	col('date').alias('date_5'),
	col('cross_device_firstAdvEngine'),
	col('cross_device_firstClickBannerGroupName'),
	col('cross_device_firstCurrencyID'),
	col('cross_device_firstDirectBannerGroup').cast(LongType()),
	col('cross_device_firstDirectClickBanner'),
	col('cross_device_firstDirectClickBannerName'),
	col('cross_device_firstDirectClickOrder').cast(LongType()),
	col('cross_device_firstDirectClickOrderName'),
	col('cross_device_firstDirectConditionType'),
	col('cross_device_firstDirectPhraseOrCond'),
	col('cross_device_firstDirectPlatform'),
	col('cross_device_firstDirectPlatformType'),
	col('cross_device_firstGCLID'),
	col('cross_device_firsthasGCLID').cast(LongType()),
	col('cross_device_firstMessenger'),
	col('cross_device_firstRecommendationSystem'),
	col('cross_device_firstReferalSource'),
	col('cross_device_firstSearchEngine'),
	col('cross_device_firstSearchEngineRoot'),
	col('cross_device_firstSocialNetwork'),
	col('cross_device_firstSocialNetworkProfile'),
	col('cross_device_firstTrafficSource'),
	col('cross_device_firstUTMCampaign'),
	col('cross_device_firstUTMContent'),
	col('cross_device_firstUTMMedium'),
	col('cross_device_firstUTMSource'),
	col('cross_device_firstUTMTerm')
).filter("visitID_5 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_5") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_5_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),