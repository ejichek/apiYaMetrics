from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_goals").getOrCreate()

hits_goals_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('automaticAdvEngine', StringType(), True),
	StructField('automaticClickBannerGroupName', StringType(), True),
	StructField('automaticCurrencyID', StringType(), True),
	StructField('automaticDirectBannerGroup', StringType(), True),
	StructField('automaticDirectClickBanner', StringType(), True),
	StructField('automaticDirectClickBannerName', StringType(), True),
	StructField('automaticDirectClickOrder', StringType(), True),
	StructField('automaticDirectClickOrderName', StringType(), True),
	StructField('automaticDirectConditionType', StringType(), True),
	StructField('automaticDirectPhraseOrCond', StringType(), True),
	StructField('automaticDirectPlatform', StringType(), True),
	StructField('automaticDirectPlatformType', StringType(), True),
	StructField('automaticGCLID', StringType(), True),
	StructField('automatichasGCLID', StringType(), True),
	StructField('automaticMessenger', StringType(), True),
	StructField('automaticRecommendationSystem', StringType(), True),
	StructField('automaticReferalSource', StringType(), True),
	StructField('automaticSearchEngine', StringType(), True),
	StructField('automaticSearchEngineRoot', StringType(), True),
	StructField('automaticSocialNetwork', StringType(), True),
	StructField('automaticSocialNetworkProfile', StringType(), True),
	StructField('automaticTrafficSource', StringType(), True),
	StructField('automaticUTMCampaign', StringType(), True),
	StructField('automaticUTMContent', StringType(), True),
	StructField('automaticUTMMedium', StringType(), True),
	StructField('automaticUTMSource', StringType(), True),
	StructField('automaticUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(hits_goals_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_3'),
	col('date').alias('date_3'),
	col('automaticAdvEngine'),
	col('automaticClickBannerGroupName'),
	col('automaticCurrencyID'),
	col('automaticDirectBannerGroup').cast(LongType()),
	col('automaticDirectClickBanner'),
	col('automaticDirectClickBannerName'),
	col('automaticDirectClickOrder').cast(LongType()),
	col('automaticDirectClickOrderName'),
	col('automaticDirectConditionType'),
	col('automaticDirectPhraseOrCond'),
	col('automaticDirectPlatform'),
	col('automaticDirectPlatformType'),
	col('automaticGCLID'),
	col('automatichasGCLID').cast(LongType()),
	col('automaticMessenger'),
	col('automaticRecommendationSystem'),
	col('automaticReferalSource'),
	col('automaticSearchEngine'),
	col('automaticSearchEngineRoot'),
	col('automaticSocialNetwork'),
	col('automaticSocialNetworkProfile'),
	col('automaticTrafficSource'),
	col('automaticUTMCampaign'),
	col('automaticUTMContent'),
	col('automaticUTMMedium'),
	col('automaticUTMSource'),
	col('automaticUTMTerm')
).filter("visitID_3 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_3") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_3_Goals")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
