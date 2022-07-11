import pandas as pd
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from google.cloud import bigquery
from datetime import datetime
from pyspark.sql.functions import col,isnan, when, count
import os
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
import seaborn as sns
import matplotlib.pyplot as plt
from google.cloud import bigquery
import sys
from pyspark.ml.feature import VectorAssembler
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import *
from pyspark.ml.classification import RandomForestClassificationModel
from sklearn.metrics import confusion_matrix


spark = SparkSession.builder.appName('pyspark-retail-forecast-ml').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar').getOrCreate()

#Reading the arguments and storing them in variables
project_name=<<your_project_name>>
dataset_name=<<your_bq_dataset_name>>
bucket_name=<<your_code_bucket>>
user_name=<<your_username_here>>

bq_client = bigquery.Client(project=project_name)

## Data Preparation

#Reading the input datasets from GCS buckets
aisles_df = spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/aisles.csv")
departments_df= spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/departments.csv")
orders_df = spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/orders.csv")
products_df = spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/products.csv")
train_df = spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/order_products__train.csv")
prior_df = spark.read.option("header",True).csv("gs://"+bucket_name+"/retail_forecast_vertex_ai/01-datasets/order_products__prior.csv")

aisles_df.show(2)
departments_df.show(2)
orders_df.show(2)
products_df.show(2)
train_df.show(2)
prior_df.show(2)


# Creating a DataFrame with the orders and the products that have been purchased on prior orders (op)
op=orders_df.join(prior_df,orders_df.order_id ==  prior_df.order_id,"inner")
op.show(2)


#Removing duplicate entries
df_cols = op.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = op.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
op=df.drop(*cols_to_remove)

# Create distinct groups for each user, identify the highest order number in each group, save the new column to a DataFrame
op = op.withColumn("order_number", op["order_number"].cast(IntegerType()))
user=op.groupBy("user_id")   .max("order_number")   .select(col("user_id"),col("max(order_number)").alias("u_total_orders"))
user.show(2)


# How frequent a customer has reordered products
op = op.withColumn("reordered", op["reordered"].cast(IntegerType()))
u_reorder=op.groupBy("user_id")   .max("reordered")   .select(col("user_id"),col("max(reordered)").alias("u_reordered_ratio"))
u_reorder.show(2)

user=user.join(u_reorder,user.user_id == u_reorder.user_id,"left")

#Removing duplicate entries
df_cols = user.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = user.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
user=df.drop(*cols_to_remove)
user.show(2)

#Extrcting product and order information from customer order history
op.createOrReplaceTempView("DATA")
prd=spark.sql("SELECT product_id,count(order_id) as p_total_purchases  FROM DATA GROUP BY product_id")
prd.show(2)

p_reorder=op.groupBy("product_id")   .avg("reordered")   .select(col("product_id"),col("avg(reordered)").alias("p_reorder_ratio"))
p_reorder.show(2)

prd=prd.join(p_reorder,prd.product_id == p_reorder.product_id,"left")

#Removing duplicate entries
df_cols = prd.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = prd.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
prd=df.drop(*cols_to_remove)
prd.show(2)
prd.na.fill(value=0,subset=["p_reorder_ratio"])

# Create distinct groups for each combination of user and product, count orders, save the result for each user X product to a new DataFrame
op.createOrReplaceTempView("DATA0")
upx=spark.sql("SELECT user_id,product_id,count(order_id) as uxp_total_bought  FROM DATA0 GROUP BY user_id,product_id")
upx.show(2)


# How frequently a customer bought a product after its first purchase
op.createOrReplaceTempView("DATA1")
times=spark.sql("SELECT user_id,product_id,count(order_id) as Times_Bought_N  FROM DATA1 GROUP BY user_id,product_id")
times.show(2)

total_orders=op.groupBy("user_id")   .max("order_number")   .select(col("user_id"),col("max(order_number)").alias("total_orders"))
total_orders.show(2)


# The order number where the customer bought a product for first time ('first_order_number')
op.createOrReplaceTempView("DATA2")
first_order_no=spark.sql("SELECT user_id,product_id,min(order_number) as first_order_number  FROM DATA2 GROUP BY user_id,product_id")
first_order_no.show(2)

span=total_orders.join(first_order_no,total_orders.user_id == first_order_no.user_id,"right")
span.show(2)


# For each product get the total orders placed since its first order ('Order_Range_D')
span=span.withColumn("Order_Range_D", span.total_orders - span.first_order_number + 1)
span.show(2)

#Removing duplicate entries
df_cols = span.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = span.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
span=df.drop(*cols_to_remove)


# Create the final ratio "uxp_reorder_ratio"
uxp_ratio=times.join(span,(times.user_id == span.user_id)&                      (times.product_id == span.product_id) ,"left")
uxp_ratio=uxp_ratio.withColumn("uxp_reorder_ratio", uxp_ratio.Times_Bought_N / uxp_ratio.Order_Range_D)
cols=['Times_Bought_N', 'total_orders', 'first_order_number', 'Order_Range_D']
uxp_ratio.drop(*cols)


##Removing duplicate entries
df_cols = uxp_ratio.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = uxp_ratio.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]

uxp_ratio=df.drop(*cols_to_remove)
uxp=upx.join(uxp_ratio,(upx.user_id == uxp_ratio.user_id)&                      (upx.product_id == uxp_ratio.product_id) ,"left")
uxp_ratio.show(2)

#Removing duplicate entries
df_cols = uxp.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = uxp.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
uxp=df.drop(*cols_to_remove)

data=uxp.join(user,uxp.user_id == user.user_id,"left")

#Removing duplicate entries
df_cols = data.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = data.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]

data=df.drop(*cols_to_remove)
data=data.join(prd,data.product_id == prd.product_id,"left")


#Removing duplicate entries
df_cols = data.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = data.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
data=df.drop(*cols_to_remove)

data.show(2)



orders_future=orders_df.filter((orders_df.eval_set == "train") | (orders_df.eval_set=='test'))
orders_future=orders_future.select(orders_future['user_id'],orders_future['eval_set'],orders_future['order_id'])
orders_future.show(2)

data=data.join(orders_future,data.user_id == orders_future.user_id,"left")


##Removing duplicate entries
df_cols = data.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = data.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
data=df.drop(*cols_to_remove)
data.show(2)


#Train
data_train=data.filter((data.eval_set == "train"))
data_train.show(1)

train1=train_df.select(train_df['product_id'],train_df['order_id'],train_df['reordered'])

data_train=data_train.join(train1,(data_train.product_id == train1.product_id)&(data_train.order_id == train1.order_id),"left")


#Removing duplicate entries
df_cols = data_train.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = data_train.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
data_train=df.drop(*cols_to_remove)


# Where the previous merge, left a NaN value on reordered column means that the customers they haven't bought the product.
# We change the value on them to 0.
data_train.na.fill(value=0,subset=["reordered"])


# We remove all non-predictor variables
cols_to_remove=['eval_set', 'order_id']
data_train=data_train.drop(*cols_to_remove)
data_train=data_train.fillna(value='0',subset=["reordered"])
data_train.groupBy('reordered').count().orderBy('count').show()
data_train=data_train.limit(100000)
data_train.groupBy('reordered').count().orderBy('count').show()

data_train.printSchema()

data_train = data_train.withColumn("product_id", data_train["product_id"].cast(IntegerType()))
data_train = data_train.withColumn("user_id", data_train["user_id"].cast(IntegerType()))
data_train = data_train.withColumn("reordered", data_train["reordered"].cast(IntegerType()))


#Writing the training dataset to BigQuery
client = bigquery.Client()
job_config = bigquery.LoadJobConfig()
table_id1 = dataset_name+'.'+user_name+'_train_data'


d_train=data_train.toPandas()
job1 = client.load_table_from_dataframe(
    d_train, table_id1, job_config=job_config)
job1.result()


#Test

# Keep only the future orders from customers who are labelled as test
data_test=data.filter((data.eval_set == "test"))


# We remove all non-predictor variables
cols_to_remove=['eval_set', 'order_id']
data_test=data_test.drop(*cols_to_remove)

data_test.printSchema()

data_test = data_test.withColumn("product_id", data_test["product_id"].cast(IntegerType()))
data_test = data_test.withColumn("user_id", data_test["user_id"].cast(IntegerType()))

data_test.show(1)
data_test=data_test.limit(40000)

d_test=data_test.toPandas()

#Writing the evaluation dataset to BigQuery
client = bigquery.Client()
job_config = bigquery.LoadJobConfig()
table_id2=dataset_name+'.'+user_name+'_test_data'

job1 = client.load_table_from_dataframe(
    d_test, table_id2, job_config=job_config)
job1.result()

#Function to summarize data characteristics
def summarize_data(df):
    print("\nOverview")
    print(df.head())
    print("\nSummary")
    print(df.describe())
    print("\nNull Values")
    print("\nCount")
    print(df.count())

summarize_data(aisles_df)
summarize_data(orders_df)
summarize_data(departments_df)

spark.stop()

## Modeling

spark = SparkSession.builder.appName('pyspark-retail-forecast-ml').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar').getOrCreate()

train_df = spark.read \
  .format('bigquery') \
  .load(project_name+'.'+dataset_name+'.'+user_name+'_train_data')

train_df.show(2)
train_df.printSchema()

#Selecting the key features required for the Random Forest Model
features=['uxp_total_bought',
 'Times_Bought_N',
 'total_orders',
 'first_order_number',
 'Order_Range_D',
 'uxp_reorder_ratio',
 'u_total_orders',
 'u_reordered_ratio',
 'p_total_purchases',
 'p_reorder_ratio'   ]

#Defining and transforming the Vector Assembler on the input data
assembler = VectorAssembler(inputCols=features,outputCol='features')
va_df=assembler.transform(train_df)

#Defining and transforming the String Indexer on the input data
indexer = StringIndexer(inputCol = 'reordered', outputCol = 'label')
i_df = indexer.fit(va_df).transform(va_df)

i_df.printSchema()
i_df.show(2)

#Splitting the input data into train and test datasets
splits = i_df.randomSplit([0.6,0.4],1)
train_df = splits[0]
test_df = splits[1]
train_df.count(), test_df.count(), i_df.count()

#Defining and training the Random Forest Classification Model
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
rfModel = rf.fit(train_df)

#Running the Random Forest Model on test data
predictions = rfModel.transform(test_df)

#Evaluating the model performance metrics
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Accuracy = %s" % (accuracy))
print("Test Error = %s" % (1.0 - accuracy))

#Writing the model output to BigQuery
spark.conf.set("parentProject", project_name)
bucket = bucket_name
spark.conf.set("temporaryGcsBucket",bucket)
predictions.write.format('bigquery') \
.mode("overwrite")\
.option('table', project_name+':'+dataset_name+'.'+user_name+'_predictions_data') \
.save()

#Saving the model to Google Cloud Storage
rfModel.write().overwrite().save('gs://'+bucket_name+'/model/rf_model.model')
spark.stop()

print('Modeling Completed!')

## Model Evaluation

spark = SparkSession.builder.appName('pyspark-retail-forecast-ml').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar').getOrCreate()

#Reading the evaluation data from BigQuery
test_df = spark.read \
  .format('bigquery') \
  .load(project_name+'.'+dataset_name+'.'+user_name+'_test_data')

bq_client = bigquery.Client(project=project_name)
test_df.show(2)


#Loading the Random Forest Classifier Model from GCS:
model_1 = RandomForestClassificationModel.load(os.path.join('gs://'+bucket_name+'/model/rf_model.model'))

#Numerical columns that are to be passed in Vector Assembler
features=['uxp_total_bought',
 'Times_Bought_N',
 'total_orders',
 'first_order_number',
 'Order_Range_D',
 'uxp_reorder_ratio',
 'u_total_orders',
 'u_reordered_ratio',
 'p_total_purchases',
 'p_reorder_ratio'   ]

assembler = VectorAssembler(inputCols=features,outputCol='features')
va_df=assembler.transform(test_df)

#Running the model on the evaluation dataset
test=model_1.transform(va_df)

#Denormalizing the model evaluation output dataset
test1=test.select(['user_id','prediction'])
test2=test1.join(test_df,test1.user_id== test_df.user_id,'inner')
test2.show(2)


#Removing Duplicate columns
df_cols = test2.columns
duplicate_col_index = list(set([df_cols.index(c) for c in df_cols if df_cols.count(c) == 2]))
for i in duplicate_col_index:
    df_cols[i] = df_cols[i] + '_duplicated'
df = test2.toDF(*df_cols)
cols_to_remove = [c for c in df_cols if '_duplicated' in c]
test2=df.drop(*cols_to_remove)
test2=test2.select(['user_id','product_id','prediction'])
test2.show(5)


#Writting the output to BigQuery
spark.conf.set("parentProject", project_name)
bucket = bucket_name
spark.conf.set("temporaryGcsBucket",bucket)
test2.write.format('bigquery') \
.mode("overwrite")\
.option('table', project_name+':'+dataset_name+'.'+user_name+'_eval_output') \
.save()

print('Job Completed!')
