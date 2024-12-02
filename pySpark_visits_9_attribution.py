from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_9_attribution").getOrCreate()

visits_9_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('firstAdvEngine', StringType(), True),
	StructField('firstClickBannerGroupName', StringType(), True),
	StructField('firstCurrencyID', StringType(), True),
	StructField('firstDirectBannerGroup', StringType(), True),
	StructField('firstDirectClickBanner', StringType(), True),
	StructField('firstDirectClickBannerName', StringType(), True),
	StructField('firstDirectClickOrder', StringType(), True),
	StructField('firstDirectClickOrderName', StringType(), True),
	StructField('firstDirectConditionType', StringType(), True),
	StructField('firstDirectPhraseOrCond', StringType(), True),
	StructField('firstDirectPlatform', StringType(), True),
	StructField('firstDirectPlatformType', StringType(), True),
	StructField('firstGCLID', StringType(), True),
	StructField('firsthasGCLID', StringType(), True),
	StructField('firstMessenger', StringType(), True),
	StructField('firstRecommendationSystem', StringType(), True),
	StructField('firstReferalSource', StringType(), True),
	StructField('firstSearchEngine', StringType(), True),
	StructField('firstSearchEngineRoot', StringType(), True),
	StructField('firstSocialNetwork', StringType(), True),
	StructField('firstSocialNetworkProfile', StringType(), True),
	StructField('firstTrafficSource', StringType(), True),
	StructField('firstUTMCampaign', StringType(), True),
	StructField('firstUTMContent', StringType(), True),
	StructField('firstUTMMedium', StringType(), True),
	StructField('firstUTMSource', StringType(), True),
	StructField('firstUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visits_9_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_9'),
	col('date').alias('date_9'),
	col('firstAdvEngine'),
	col('firstClickBannerGroupName'),
	col('firstCurrencyID'),
	col('firstDirectBannerGroup').cast(LongType()),
	col('firstDirectClickBanner'),
	col('firstDirectClickBannerName'),
	col('firstDirectClickOrder').cast(LongType()),
	col('firstDirectClickOrderName'),
	col('firstDirectConditionType'),
	col('firstDirectPhraseOrCond'),
	col('firstDirectPlatform'),
	col('firstDirectPlatformType'),
	col('firstGCLID'),
	col('firsthasGCLID').cast(LongType()),
	col('firstMessenger'),
	col('firstRecommendationSystem'),
	col('firstReferalSource'),
	col('firstSearchEngine'),
	col('firstSearchEngineRoot'),
	col('firstSocialNetwork'),
	col('firstSocialNetworkProfile'),
	col('firstTrafficSource'),
	col('firstUTMCampaign'),
	col('firstUTMContent'),
	col('firstUTMMedium'),
	col('firstUTMSource'),
	col('firstUTMTerm')
).filter("visitID_9 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_9") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_9_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
