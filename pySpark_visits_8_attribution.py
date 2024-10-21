from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visits_8_attribution").getOrCreate()

visits_8_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('cross_device_last_yandex_direct_clickAdvEngine', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickClickBannerGroupName', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickCurrencyID', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectBannerGroup', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectClickBanner', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectClickBannerName', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectClickOrder', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectClickOrderName', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectConditionType', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectPhraseOrCond', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectPlatform', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickDirectPlatformType', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickGCLID', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickhasGCLID', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickMessenger', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickRecommendationSystem', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickReferalSource', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickSearchEngine', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickSearchEngineRoot', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickSocialNetwork', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickSocialNetworkProfile', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickTrafficSource', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickUTMCampaign', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickUTMContent', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickUTMMedium', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickUTMSource', StringType(), True),
	StructField('cross_device_last_yandex_direct_clickUTMTerm', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(visits_8_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_8'),
	col('date').alias('date_8'),
	col('cross_device_last_yandex_direct_clickAdvEngine'),
	col('cross_device_last_yandex_direct_clickClickBannerGroupName'),
	col('cross_device_last_yandex_direct_clickCurrencyID'),
	col('cross_device_last_yandex_direct_clickDirectBannerGroup').cast(LongType()),
	col('cross_device_last_yandex_direct_clickDirectClickBanner'),
	col('cross_device_last_yandex_direct_clickDirectClickBannerName'),
	col('cross_device_last_yandex_direct_clickDirectClickOrder').cast(LongType()),
	col('cross_device_last_yandex_direct_clickDirectClickOrderName'),
	col('cross_device_last_yandex_direct_clickDirectConditionType'),
	col('cross_device_last_yandex_direct_clickDirectPhraseOrCond'),
	col('cross_device_last_yandex_direct_clickDirectPlatform'),
	col('cross_device_last_yandex_direct_clickDirectPlatformType'),
	col('cross_device_last_yandex_direct_clickGCLID'),
	col('cross_device_last_yandex_direct_clickhasGCLID').cast(LongType()),
	col('cross_device_last_yandex_direct_clickMessenger'),
	col('cross_device_last_yandex_direct_clickRecommendationSystem'),
	col('cross_device_last_yandex_direct_clickReferalSource'),
	col('cross_device_last_yandex_direct_clickSearchEngine'),
	col('cross_device_last_yandex_direct_clickSearchEngineRoot'),
	col('cross_device_last_yandex_direct_clickSocialNetwork'),
	col('cross_device_last_yandex_direct_clickSocialNetworkProfile'),
	col('cross_device_last_yandex_direct_clickTrafficSource'),
	col('cross_device_last_yandex_direct_clickUTMCampaign'),
	col('cross_device_last_yandex_direct_clickUTMContent'),
	col('cross_device_last_yandex_direct_clickUTMMedium'),
	col('cross_device_last_yandex_direct_clickUTMSource'),
	col('cross_device_last_yandex_direct_clickUTMTerm')
).filter("visitID_8 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_8") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_8_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),