from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_12_attribution").getOrCreate()

visits_12_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('lastsignAdvEngine', StringType(), True),
	StructField('lastsignClickBannerGroupName', StringType(), True),
	StructField('lastsignCurrencyID', StringType(), True),
	StructField('lastsignDirectBannerGroup', StringType(), True),
	StructField('lastsignDirectClickBanner', StringType(), True),
	StructField('lastsignDirectClickBannerName', StringType(), True),
	StructField('lastsignDirectClickOrder', StringType(), True),
	StructField('lastsignDirectClickOrderName', StringType(), True),
	StructField('lastsignDirectConditionType', StringType(), True),
	StructField('lastsignDirectPhraseOrCond', StringType(), True),
	StructField('lastsignDirectPlatform', StringType(), True),
	StructField('lastsignDirectPlatformType', StringType(), True),
	StructField('lastsignGCLID', StringType(), True),
	StructField('lastsignhasGCLID', StringType(), True),
	StructField('lastsignMessenger', StringType(), True),
	StructField('lastsignRecommendationSystem', StringType(), True),
	StructField('lastsignReferalSource', StringType(), True),
	StructField('lastsignSearchEngine', StringType(), True),
	StructField('lastsignSearchEngineRoot', StringType(), True),
	StructField('lastsignSocialNetwork', StringType(), True),
	StructField('lastsignSocialNetworkProfile', StringType(), True),
	StructField('lastsignTrafficSource', StringType(), True),
	StructField('lastsignUTMCampaign', StringType(), True),
	StructField('lastsignUTMContent', StringType(), True),
	StructField('lastsignUTMMedium', StringType(), True),
	StructField('lastsignUTMSource', StringType(), True),
	StructField('lastsignUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visits_12_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_12'),
	col('date').alias('date_12'),
	col('lastsignAdvEngine'),
	col('lastsignClickBannerGroupName'),
	col('lastsignCurrencyID'),
	col('lastsignDirectBannerGroup').cast(LongType()),
	col('lastsignDirectClickBanner'),
	col('lastsignDirectClickBannerName'),
	col('lastsignDirectClickOrder').cast(LongType()),
	col('lastsignDirectClickOrderName'),
	col('lastsignDirectConditionType'),
	col('lastsignDirectPhraseOrCond'),
	col('lastsignDirectPlatform'),
	col('lastsignDirectPlatformType'),
	col('lastsignGCLID'),
	col('lastsignhasGCLID').cast(LongType()),
	col('lastsignMessenger'),
	col('lastsignRecommendationSystem'),
	col('lastsignReferalSource'),
	col('lastsignSearchEngine'),
	col('lastsignSearchEngineRoot'),
	col('lastsignSocialNetwork'),
	col('lastsignSocialNetworkProfile'),
	col('lastsignTrafficSource'),
	col('lastsignUTMCampaign'),
	col('lastsignUTMContent'),
	col('lastsignUTMMedium'),
	col('lastsignUTMSource'),
	col('lastsignUTMTerm')
).filter("visitID_12 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_12") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_12_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
