from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_vsiits_13_attribution").getOrCreate()

vsiits_13_attribution_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('eventsProductBrand', StringType(), True),
	StructField('eventsProductCategory', StringType(), True),
	StructField('eventsProductCategory1', StringType(), True),
	StructField('eventsProductCategory2', StringType(), True),
	StructField('eventsProductCategory3', StringType(), True),
	StructField('eventsProductCategory4', StringType(), True),
	StructField('eventsProductCoupon', StringType(), True),
	StructField('eventsProductCurrency', StringType(), True),
	StructField('eventsProductDiscount', StringType(), True),
	StructField('eventsProductEventTime', StringType(), True),
	StructField('eventsProductID', StringType(), True),
	StructField('eventsProductList', StringType(), True),
	StructField('eventsProductName', StringType(), True),
	StructField('eventsProductPosition', StringType(), True),
	StructField('eventsProductPrice', StringType(), True),
	StructField('eventsProductQuantity', StringType(), True),
	StructField('eventsProductType', StringType(), True),
	StructField('eventsProductVariant', StringType(), True),
	StructField('impressionsDateTime', StringType(), True),
	StructField('impressionsProductBrand', StringType(), True),
	StructField('impressionsProductCategory', StringType(), True),
	StructField('impressionsProductCategory1', StringType(), True),
	StructField('impressionsProductCategory2', StringType(), True),
	StructField('impressionsProductCategory3', StringType(), True),
	StructField('impressionsProductCategory4', StringType(), True),
	StructField('impressionsProductCoupon', StringType(), True),
	StructField('impressionsProductCurrency', StringType(), True),
	StructField('impressionsProductDiscount', StringType(), True),
	StructField('impressionsProductEventTime', StringType(), True),
	StructField('impressionsProductID', StringType(), True),
	StructField('impressionsProductList', StringType(), True),
	StructField('impressionsProductName', StringType(), True),
	StructField('impressionsProductPrice', StringType(), True),
	StructField('impressionsProductQuantity', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "|") \
    .schema(vsiits_13_attribution_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_13'),
	col('date').alias('date_13'),
	split(regexp_replace(col('eventsProductBrand'), r'(\]|\[)', ''), ',').alias('eventsProductBrand'),
	split(regexp_replace(col('eventsProductCategory'), r'(\]|\[)', ''), ',').alias('eventsProductCategory'),
	split(regexp_replace(col('eventsProductCategory1'), r'(\]|\[)', ''), ',').alias('eventsProductCategory1'),
	split(regexp_replace(col('eventsProductCategory2'), r'(\]|\[)', ''), ',').alias('eventsProductCategory2'),
	split(regexp_replace(col('eventsProductCategory3'), r'(\]|\[)', ''), ',').alias('eventsProductCategory3'),
	split(regexp_replace(col('eventsProductCategory4'), r'(\]|\[)', ''), ',').alias('eventsProductCategory4'),
	split(regexp_replace(col('eventsProductCoupon'), r'(\]|\[)', ''), ',').alias('eventsProductCoupon'),
	split(regexp_replace(col('eventsProductCurrency'), r'(\]|\[)', ''), ',').alias('eventsProductCurrency'),
	split(regexp_replace(col('eventsProductDiscount'), r'(\]|\[)', ''), ',').alias('eventsProductDiscount'),
	split(regexp_replace(col('eventsProductEventTime'), r'(\]|\[)', ''), ',').alias('eventsProductEventTime'),
	split(regexp_replace(col('eventsProductID'), r'(\]|\[)', ''), ',').alias('eventsProductID'),
	split(regexp_replace(col('eventsProductList'), r'(\]|\[)', ''), ',').alias('eventsProductList'),
	split(regexp_replace(col('eventsProductName'), r'(\]|\[)', ''), ',').alias('eventsProductName'),
	split(regexp_replace(col('eventsProductPosition'), r'(\]|\[)', ''), ',').alias('eventsProductPosition'),
	split(regexp_replace(col('eventsProductPrice'), r'(\]|\[)', ''), ',').alias('eventsProductPrice'),
	split(regexp_replace(col('eventsProductQuantity'), r'(\]|\[)', ''), ',').alias('eventsProductQuantity'),
	split(regexp_replace(col('eventsProductType'), r'(\]|\[)', ''), ',').alias('eventsProductType'),
	split(regexp_replace(col('eventsProductVariant'), r'(\]|\[)', ''), ',').alias('eventsProductVariant'),
	split(regexp_replace(col('impressionsDateTime'), r'(\]|\[)', ''), ',').alias('impressionsDateTime'),
	split(regexp_replace(col('impressionsProductBrand'), r'(\]|\[)', ''), ',').alias('impressionsProductBrand'),
	split(regexp_replace(col('impressionsProductCategory'), r'(\]|\[)', ''), ',').alias('impressionsProductCategory'),
	split(regexp_replace(col('impressionsProductCategory1'), r'(\]|\[)', ''), ',').alias('impressionsProductCategory1'),
	split(regexp_replace(col('impressionsProductCategory2'), r'(\]|\[)', ''), ',').alias('impressionsProductCategory2'),
	split(regexp_replace(col('impressionsProductCategory3'), r'(\]|\[)', ''), ',').alias('impressionsProductCategory3'),
	split(regexp_replace(col('impressionsProductCategory4'), r'(\]|\[)', ''), ',').alias('impressionsProductCategory4'),
	split(regexp_replace(col('impressionsProductCoupon'), r'(\]|\[)', ''), ',').alias('impressionsProductCoupon'),
	split(regexp_replace(col('impressionsProductCurrency'), r'(\]|\[)', ''), ',').alias('impressionsProductCurrency'),
	split(regexp_replace(col('impressionsProductDiscount'), r'(\]|\[)', ''), ',').alias('impressionsProductDiscount'),
	split(regexp_replace(col('impressionsProductEventTime'), r'(\]|\[)', ''), ',').alias('impressionsProductEventTime'),
	split(regexp_replace(col('impressionsProductID'), r'(\]|\[)', ''), ',').alias('impressionsProductID'),
	split(regexp_replace(col('impressionsProductList'), r'(\]|\[)', ''), ',').alias('impressionsProductList'),
	split(regexp_replace(col('impressionsProductName'), r'(\]|\[)', ''), ',').alias('impressionsProductName'),
	split(regexp_replace(col('impressionsProductPrice'), r'(\]|\[)', ''), ',').alias('impressionsProductPrice'),
	split(regexp_replace(col('impressionsProductQuantity'), r'(\]|\[)', ''), ',').alias('impressionsProductQuantity')
).filter("visitID_13 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_13") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_13_Attribution")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),