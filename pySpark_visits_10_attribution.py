from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_10_attribution").getOrCreate()

visits_10_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('lastAdvEngine', StringType(), True),
	StructField('lastClickBannerGroupName', StringType(), True),
	StructField('lastCurrencyID', StringType(), True),
	StructField('lastDirectBannerGroup', StringType(), True),
	StructField('lastDirectClickBanner', StringType(), True),
	StructField('lastDirectClickBannerName', StringType(), True),
	StructField('lastDirectClickOrder', StringType(), True),
	StructField('lastDirectClickOrderName', StringType(), True),
	StructField('lastDirectConditionType', StringType(), True),
	StructField('lastDirectPhraseOrCond', StringType(), True),
	StructField('lastDirectPlatform', StringType(), True),
	StructField('lastDirectPlatformType', StringType(), True),
	StructField('lastGCLID', StringType(), True),
	StructField('lasthasGCLID', StringType(), True),
	StructField('lastMessenger', StringType(), True),
	StructField('lastRecommendationSystem', StringType(), True),
	StructField('lastReferalSource', StringType(), True),
	StructField('lastSearchEngine', StringType(), True),
	StructField('lastSearchEngineRoot', StringType(), True),
	StructField('lastSocialNetwork', StringType(), True),
	StructField('lastSocialNetworkProfile', StringType(), True),
	StructField('lastTrafficSource', StringType(), True),
	StructField('lastUTMCampaign', StringType(), True),
	StructField('lastUTMContent', StringType(), True),
	StructField('lastUTMMedium', StringType(), True),
	StructField('lastUTMSource', StringType(), True),
	StructField('lastUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visits_10_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_10'),
	col('date').alias('date_10'),
	col('lastAdvEngine'),
	col('lastClickBannerGroupName'),
	col('lastCurrencyID'),
	col('lastDirectBannerGroup').cast(LongType()),
	col('lastDirectClickBanner'),
	col('lastDirectClickBannerName'),
	col('lastDirectClickOrder').cast(LongType()),
	col('lastDirectClickOrderName'),
	col('lastDirectConditionType'),
	col('lastDirectPhraseOrCond'),
	col('lastDirectPlatform'),
	col('lastDirectPlatformType'),
	col('lastGCLID'),
	col('lasthasGCLID').cast(LongType()),
	col('lastMessenger'),
	col('lastRecommendationSystem'),
	col('lastReferalSource'),
	col('lastSearchEngine'),
	col('lastSearchEngineRoot'),
	col('lastSocialNetwork'),
	col('lastSocialNetworkProfile'),
	col('lastTrafficSource'),
	col('lastUTMCampaign'),
	col('lastUTMContent'),
	col('lastUTMMedium'),
	col('lastUTMSource'),
	col('lastUTMTerm')
).filter("visitID_10 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_10") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_10_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
