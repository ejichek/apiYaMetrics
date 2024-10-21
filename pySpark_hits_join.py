from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("hits_join").getOrCreate()

df_basic = spark.read.orc("/user/azhalybin/airflow/test/orc_hits_basic").select("*")
df_trafficSource = spark.read.orc("/user/azhalybin/airflow/test/orc_hits_TrafficSource").select("*")
df_ecommerce_EventParams_EventType = spark.read.orc("/user/azhalybin/airflow/test/orc_hits_Ecommerce_EventParams_EventType").select("*")
df_device = spark.read.orc("/user/azhalybin/airflow/test/orc_hits_Device").select("*")

df_all_join = df_basic   \
	.join(df_trafficSource, df_basic.watchID_1 == df_trafficSource.watchID_2, "full")   \
	.join(df_ecommerce_EventParams_EventType, df_basic.watchID_1 == df_ecommerce_EventParams_EventType.watchID_3, "full")   \
	.join(df_device, df_basic.watchID_1 == df_device.watchID_4, "full")

df_all_hits = df_all_join.select(
	col('watchID_1').alias('watchID'),
	col('pageViewID'),
	col('counterID'),
	col('clientID'),
	col('counterUserIDHash'),
	col('date_1').alias('date'),
	col('dateTime'),
	col('title'),
	col('goalsID'),
	col('URL'),
	col('referer'),
	col('UTMCampaign'),
	col('UTMContent'),
	col('UTMMedium'),
	col('UTMSource'),
	col('UTMTerm'),
	col('operatingSystem'),
	col('from'),
	col('hasGCLID'),
	col('GCLID'),
	col('lastTrafficSource'),
	col('lastSearchEngineRoot'),
	col('lastSearchEngine'),
	col('lastAdvEngine'),
	col('lastSocialNetwork'),
	col('lastSocialNetworkProfile'),
	col('recommendationSystem'),
	col('messenger'),
	col('browser'),
	col('browserMajorVersion'),
	col('browserMinorVersion'),
	col('browserCountry'),
	col('browserEngine'),
	col('browserEngineVersion1'),
	col('browserEngineVersion2'),
	col('browserEngineVersion3'),
	col('browserEngineVersion4'),
	col('browserLanguage'),
	col('clientTimeZone'),
	col('cookieEnabled'),
	col('deviceCategory'),
	col('javascriptEnabled'),
	col('mobilePhone'),
	col('mobilePhoneModel'),
	col('operatingSystemRoot'),
	col('physicalScreenHeight'),
	col('physicalScreenWidth'),
	col('screenColors'),
	col('screenFormat'),
	col('screenHeight'),
	col('screenOrientation'),
	col('screenWidth'),
	col('windowClientHeight'),
	col('windowClientWidth'),
	col('ipAddress'),
	col('regionCity'),
	col('regionCountry'),
	col('regionCityID'),
	col('regionCountryID'),
	col('isPageView'),
	col('isTurboPage'),
	col('isTurboApp'),
	col('iFrame'),
	col('link'),
	col('download'),
	col('notBounce'),
	col('artificial'),
	col('ecommerce'),
	col('params'),
	col('parsedParamsKey1'),
	col('parsedParamsKey2'),
	col('parsedParamsKey3'),
	col('parsedParamsKey4'),
	col('parsedParamsKey5'),
	col('parsedParamsKey6'),
	col('parsedParamsKey7'),
	col('parsedParamsKey8'),
	col('parsedParamsKey9'),
	col('parsedParamsKey10'),
	col('httpError'),
	col('shareService'),
	col('shareURL'),
	col('shareTitle')
).distinct()

df_all_hits.write.format("orc") \
			.mode("append") \
			.partitionBy("date") \
			.option("maxRecordsPerFile", 150000) \
			.save("/user/azhalybin/airflow/test/orc_hits_join")