from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_11_attribution").getOrCreate()

visits_11_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('last_yandex_direct_clickAdvEngine', StringType(), True),
	StructField('last_yandex_direct_clickClickBannerGroupName', StringType(), True),
	StructField('last_yandex_direct_clickCurrencyID', StringType(), True),
	StructField('last_yandex_direct_clickDirectBannerGroup', StringType(), True),
	StructField('last_yandex_direct_clickDirectClickBanner', StringType(), True),
	StructField('last_yandex_direct_clickDirectClickBannerName', StringType(), True),
	StructField('last_yandex_direct_clickDirectClickOrder', StringType(), True),
	StructField('last_yandex_direct_clickDirectClickOrderName', StringType(), True),
	StructField('last_yandex_direct_clickDirectConditionType', StringType(), True),
	StructField('last_yandex_direct_clickDirectPhraseOrCond', StringType(), True),
	StructField('last_yandex_direct_clickDirectPlatform', StringType(), True),
	StructField('last_yandex_direct_clickDirectPlatformType', StringType(), True),
	StructField('last_yandex_direct_clickGCLID', StringType(), True),
	StructField('last_yandex_direct_clickhasGCLID', StringType(), True),
	StructField('last_yandex_direct_clickMessenger', StringType(), True),
	StructField('last_yandex_direct_clickRecommendationSystem', StringType(), True),
	StructField('last_yandex_direct_clickReferalSource', StringType(), True),
	StructField('last_yandex_direct_clickSearchEngine', StringType(), True),
	StructField('last_yandex_direct_clickSearchEngineRoot', StringType(), True),
	StructField('last_yandex_direct_clickSocialNetwork', StringType(), True),
	StructField('last_yandex_direct_clickSocialNetworkProfile', StringType(), True),
	StructField('last_yandex_direct_clickTrafficSource', StringType(), True),
	StructField('last_yandex_direct_clickUTMCampaign', StringType(), True),
	StructField('last_yandex_direct_clickUTMContent', StringType(), True),
	StructField('last_yandex_direct_clickUTMMedium', StringType(), True),
	StructField('last_yandex_direct_clickUTMSource', StringType(), True),
	StructField('last_yandex_direct_clickUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visits_11_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_11'),
	col('date').alias('date_11'),
	col('last_yandex_direct_clickAdvEngine'),
	col('last_yandex_direct_clickClickBannerGroupName'),
	col('last_yandex_direct_clickCurrencyID'),
	col('last_yandex_direct_clickDirectBannerGroup').cast(LongType()),
	col('last_yandex_direct_clickDirectClickBanner'),
	col('last_yandex_direct_clickDirectClickBannerName'),
	col('last_yandex_direct_clickDirectClickOrder').cast(LongType()),
	col('last_yandex_direct_clickDirectClickOrderName'),
	col('last_yandex_direct_clickDirectConditionType'),
	col('last_yandex_direct_clickDirectPhraseOrCond'),
	col('last_yandex_direct_clickDirectPlatform'),
	col('last_yandex_direct_clickDirectPlatformType'),
	col('last_yandex_direct_clickGCLID'),
	col('last_yandex_direct_clickhasGCLID').cast(LongType()),
	col('last_yandex_direct_clickMessenger'),
	col('last_yandex_direct_clickRecommendationSystem'),
	col('last_yandex_direct_clickReferalSource'),
	col('last_yandex_direct_clickSearchEngine'),
	col('last_yandex_direct_clickSearchEngineRoot'),
	col('last_yandex_direct_clickSocialNetwork'),
	col('last_yandex_direct_clickSocialNetworkProfile'),
	col('last_yandex_direct_clickTrafficSource'),
	col('last_yandex_direct_clickUTMCampaign'),
	col('last_yandex_direct_clickUTMContent'),
	col('last_yandex_direct_clickUTMMedium'),
	col('last_yandex_direct_clickUTMSource'),
	col('last_yandex_direct_clickUTMTerm')
).filter("visitID_11 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_11") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_11_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
