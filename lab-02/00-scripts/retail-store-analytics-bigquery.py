
import pyspark
from datetime import datetime
from pyspark.sql.functions import col,isnan, when, count
import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql.window import Window
from pyspark.sql.functions import col,avg,sum,min,max,row_number
from pyspark.sql.functions import round
import sys

#Reading the arguments and storing them in variables

project_name=sys.argv[1]
dataset_name=sys.argv[2]
bucket_name=sys.argv[3]
user_name=sys.argv[4]


# Building the Spark Session
spark = SparkSession.builder.appName('pyspark-retail-inventory').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar').getOrCreate()

# Creating schema for the external tables
spark.sql("""DROP TABLE IF EXISTS aisles""");

spark.sql(""" CREATE EXTERNAL TABLE aisles (
  aisle_id STRING COMMENT 'aisle_id',
  aisle STRING COMMENT 'aisle'
) USING CSV
OPTIONS (path "gs://{}/retail_store_analytics_bigquery_metastore/01-datasets/aisles",
        delimiter ",",
        header "true")
""".format(bucket_name))


spark.sql("""DROP TABLE IF EXISTS departments""");

spark.sql(""" CREATE EXTERNAL TABLE departments (
  department_id STRING COMMENT 'department_id',
  department STRING COMMENT 'department'
) USING CSV
OPTIONS (path "gs://{}/retail_store_analytics_bigquery_metastore/01-datasets/departments",
        delimiter ",",
        header "true")
""".format(bucket_name))


spark.sql("""DROP TABLE IF EXISTS orders""");
spark.sql(""" CREATE EXTERNAL TABLE orders (
  order_id STRING COMMENT 'order_id',
  user_id STRING COMMENT 'user_id',
  eval_set STRING COMMENT 'eval_set',
  order_number STRING COMMENT 'order_number',
  order_dow STRING COMMENT 'order_dow',
  order_hour_of_day STRING COMMENT 'order_hour_of_day',
  days_since_prior_order STRING COMMENT 'days_since_prior_order'  
) USING CSV
OPTIONS (path "gs://{}/retail_store_analytics_bigquery_metastore/01-datasets/orders",
        delimiter ",",
        header "true")
""".format(bucket_name))


spark.sql("""DROP TABLE IF EXISTS products""");

spark.sql(""" CREATE EXTERNAL TABLE products (
  product_id STRING COMMENT 'product_id',
  product_name STRING COMMENT 'product_name',
  aisle_id STRING COMMENT 'aisle_id',
  department_id STRING COMMENT 'department_id'
) USING CSV
OPTIONS (path "gs://{}/retail_store_analytics_bigquery_metastore/01-datasets/products",
        delimiter ",",
        header "true")
""".format(bucket_name))



spark.sql("""DROP TABLE IF EXISTS order_products""");

spark.sql(""" CREATE EXTERNAL TABLE order_products (
  order_id STRING COMMENT 'order_id',
  product_id STRING COMMENT 'product_id',
  add_to_cart_order STRING COMMENT 'add_to_cart_order',
  reordered STRING COMMENT 'reordered'
) USING CSV
OPTIONS (path "gs://{}/retail_store_analytics_bigquery_metastore/01-datasets/order_products",
        delimiter ",",
        header "true")
""".format(bucket_name))


or_df=spark.sql(""" select * from order_products op inner join orders o on op.order_id =o.order_id """)

df_join=spark.sql(""" select * from products p inner join aisles a on p.aisle_id=a.aisle_id 
inner join departments d on p.department_id=d.department_id """)

df_join = df_join.withColumnRenamed("product_id","product_id_del")
df=df_join.join(or_df,df_join.product_id_del ==  or_df.product_id,"inner")

windowSpecAgg  = Window.partitionBy("order_dow","department","product_id")

#calculating sales per order_dow_per_department_product

df_1=df.withColumn("sales_per_dow_per_departmentproduct", sum(col("add_to_cart_order")).over(windowSpecAgg)).select("sales_per_dow_per_departmentproduct","department","product_id","aisle","p.aisle_id","order_dow").distinct()

average_windowspec=Window.partitionBy("product_id")

#calculating average sales
avg_df=df_1.withColumn("avg_sales",avg(col("sales_per_dow_per_departmentproduct")).over(average_windowspec))

#calculating inventory
inventory_df=avg_df.withColumn("inventory",round(col("avg_sales")-col("sales_per_dow_per_departmentproduct")))

# Printing test data for analysis
inventory_df.filter(col("product_id")==27845).show(20)

spark.conf.set("parentProject", project_name)
bucket = bucket_name
spark.conf.set("temporaryGcsBucket",bucket)

# writing data to bigquery
inventory_df.write.format('bigquery') .mode("overwrite").option('table', project_name+':'+dataset_name+'.'+user_name+'_inventory_data') .save()

print('Job Completed Successfully!')


