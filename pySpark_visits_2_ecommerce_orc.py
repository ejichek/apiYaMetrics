from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Download_visit_ecommerce").getOrCreate()

visit_ecommerce_schema = StructType([
	StructField('visitID', StringType(), True),
	StructField('date', DateType(), True),
	StructField('impressionsProductVariant', StringType(), True),
	StructField('impressionsURL', StringType(), True),
	StructField('productsBrand', StringType(), True),
	StructField('productsCategory', StringType(), True),
	StructField('productsCategory1', StringType(), True),
	StructField('productsCategory2', StringType(), True),
	StructField('productsCategory3', StringType(), True),
	StructField('productsCategory4', StringType(), True),
	StructField('productsCoupon', StringType(), True),
	StructField('productsCurrency', StringType(), True),
	StructField('productsDiscount', StringType(), True),
	StructField('productsEventTime', StringType(), True),
	StructField('productsID', StringType(), True),
	StructField('productsList', StringType(), True),
	StructField('productsName', StringType(), True),
	StructField('productsPosition', StringType(), True),
	StructField('productsPrice', StringType(), True),
	StructField('productsPurchaseID', StringType(), True),
	StructField('productsQuantity', StringType(), True),
	StructField('productsVariant', StringType(), True),
	StructField('promotionCreative', StringType(), True),
	StructField('promotionCreativeSlot', StringType(), True),
	StructField('promotionEventTime', StringType(), True),
	StructField('promotionID', StringType(), True),
	StructField('promotionName', StringType(), True),
	StructField('promotionPosition', StringType(), True),
	StructField('promotionType', StringType(), True),
	StructField('purchaseAffiliation', StringType(), True),
	StructField('purchaseCoupon', StringType(), True),
	StructField('purchaseCurrency', StringType(), True),
	StructField('purchaseDateTime', StringType(), True),
	StructField('purchaseID', StringType(), True),
	StructField('purchaseProductQuantity', StringType(), True),
	StructField('purchaseRevenue', StringType(), True)
])

df1 = spark.read \
    .option("delimiter", "\t") \
    .schema(visit_ecommerce_schema) \
    .csv("/user/azhalybin/airflow/test/txt/final_txt.txt")

df2 = df1.select(
	col('visitID').alias('visitID_2'),
	col('date').alias('date_2'),
	split(regexp_replace(col('impressionsProductVariant'), r'(\]|\[)', ''), ',').alias('impressionsProductVariant'),
	split(regexp_replace(col('impressionsURL'), r'(\]|\[)', ''), ',').alias('impressionsURL'),
	split(regexp_replace(col('productsBrand'), r'(\]|\[)', ''), ',').alias('productsBrand'),
	split(regexp_replace(col('productsCategory'), r'(\]|\[)', ''), ',').alias('productsCategory'),
	split(regexp_replace(col('productsCategory1'), r'(\]|\[)', ''), ',').alias('productsCategory1'),
	split(regexp_replace(col('productsCategory2'), r'(\]|\[)', ''), ',').alias('productsCategory2'),
	split(regexp_replace(col('productsCategory3'), r'(\]|\[)', ''), ',').alias('productsCategory3'),
	split(regexp_replace(col('productsCategory4'), r'(\]|\[)', ''), ',').alias('productsCategory4'),
	split(regexp_replace(col('productsCoupon'), r'(\]|\[)', ''), ',').alias('productsCoupon'),
	split(regexp_replace(col('productsCurrency'), r'(\]|\[)', ''), ',').alias('productsCurrency'),
	split(regexp_replace(col('productsDiscount'), r'(\]|\[)', ''), ',').alias('productsDiscount'),
	split(regexp_replace(col('productsEventTime'), r'(\]|\[)', ''), ',').alias('productsEventTime'),
	split(regexp_replace(col('productsID'), r'(\]|\[)', ''), ',').alias('productsID'),
	split(regexp_replace(col('productsList'), r'(\]|\[)', ''), ',').alias('productsList'),
	split(regexp_replace(col('productsName'), r'(\]|\[)', ''), ',').alias('productsName'),
	split(regexp_replace(col('productsPosition'), r'(\]|\[)', ''), ',').alias('productsPosition'),
	split(regexp_replace(col('productsPrice'), r'(\]|\[)', ''), ',').alias('productsPrice'),
	split(regexp_replace(col('productsPurchaseID'), r'(\]|\[)', ''), ',').alias('productsPurchaseID'),
	split(regexp_replace(col('productsQuantity'), r'(\]|\[)', ''), ',').alias('productsQuantity'),
	split(regexp_replace(col('productsVariant'), r'(\]|\[)', ''), ',').alias('productsVariant'),
	split(regexp_replace(col('promotionCreative'), r'(\]|\[)', ''), ',').alias('promotionCreative'),
	split(regexp_replace(col('promotionCreativeSlot'), r'(\]|\[)', ''), ',').alias('promotionCreativeSlot'),
	split(regexp_replace(col('promotionEventTime'), r'(\]|\[)', ''), ',').alias('promotionEventTime'),
	split(regexp_replace(col('promotionID'), r'(\]|\[)', ''), ',').alias('promotionID'),
	split(regexp_replace(col('promotionName'), r'(\]|\[)', ''), ',').alias('promotionName'),
	split(regexp_replace(col('promotionPosition'), r'(\]|\[)', ''), ',').alias('promotionPosition'),
	split(regexp_replace(col('promotionType'), r'(\]|\[)', ''), ',').alias('promotionType'),
	split(regexp_replace(col('purchaseAffiliation'), r'(\]|\[)', ''), ',').alias('purchaseAffiliation'),
	split(regexp_replace(col('purchaseCoupon'), r'(\]|\[)', ''), ',').alias('purchaseCoupon'),
	split(regexp_replace(col('purchaseCurrency'), r'(\]|\[)', ''), ',').alias('purchaseCurrency'),
	split(regexp_replace(col('purchaseDateTime'), r'(\]|\[)', ''), ',').alias('purchaseDateTime'),
	split(regexp_replace(col('purchaseID'), r'(\]|\[)', ''), ',').alias('purchaseID'),
	split(regexp_replace(col('purchaseProductQuantity'), r'(\]|\[)', ''), ',').alias('purchaseProductQuantity'),
	split(regexp_replace(col('purchaseRevenue'), r'(\]|\[)', ''), ',').alias('purchaseRevenue')
).filter("visitID_2 is not null")

df2.show(3)

df2.write.format("orc") \
         .mode("append") \
         .partitionBy("date_2") \
         .option("maxRecordsPerFile", 150000) \
         .save("/user/azhalybin/airflow/test/orc_visits_2_Ecommerce")

#    split(regexp_replace(col('parsedParamsKey1'), r"(\]|\[)", ""), ",").alias('parsedParamsKey1'),
