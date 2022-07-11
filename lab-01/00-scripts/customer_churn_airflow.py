import os
from airflow.models import Variable
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (DataprocCreateBatchOperator,DataprocGetBatchOperator)
from datetime import datetime
from airflow.utils.dates import days_ago
import string
import random # define the random module
S = 10  # number of characters in the string.
# call random.choices() string module to find the string in Uppercase + numeric data.
ran = ''.join(random.choices(string.digits, k = S))
PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")
subnet=models.Variable.get("subnet")
phs_server=Variable.get("phs")
code_bucket=Variable.get("code_bucket")
bq_dataset=Variable.get("bq_dataset")
umsa=Variable.get("umsa")

name=<your_name_here>

dag_name= name+"_Customer_Churn_Prediction"
service_account_id= umsa+"@"+PROJECT_ID+".iam.gserviceaccount.com"

BATCH_ID = "customer-churn-prediction-"+str(ran)

customer_churn_data_prep_url = "gs://"+code_bucket+"/customer_churn/00-scripts/customer_churn_data_prep.py"
customer_churn_model_building_url = "gs://"+code_bucket+"/customer_churn/00-scripts/customer_churn_model_building.py"
customer_churn_model_testing_url = "gs://"+code_bucket+"/customer_churn/00-scripts/customer_churn_model_testing.py"

BATCH_CONFIG1 = {
    "pyspark_batch": {
        "main_python_file_uri": customer_churn_data_prep_url,
        "args":[
          PROJECT_ID,
          bq_dataset,
          code_bucket,
          name
        ],
        "jar_file_uris": [
      "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
    ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
            },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{phs_server}"
                }
            },
        },
}
BATCH_CONFIG2 = {
    "pyspark_batch": {
        "main_python_file_uri": customer_churn_model_building_url,
        "args":[
          PROJECT_ID,
          bq_dataset,
          code_bucket,
          name
        ],
        "jar_file_uris": [
      "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
    ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
            },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{phs_server}"
                }
            },
        },
}
BATCH_CONFIG3 = {
    "pyspark_batch": {
        "main_python_file_uri": customer_churn_model_testing_url,
        "args":[
          PROJECT_ID,
          bq_dataset,
          code_bucket,
          name
        ],
        "jar_file_uris": [
      "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar"
    ]
    },
    "environment_config":{
        "execution_config":{
            "service_account": service_account_id,
            "subnetwork_uri": subnet
            },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{phs_server}"
                }
            },
        },
}
with models.DAG(
    name+"_customer_churn_prediction",
    schedule_interval=None,
    start_date = days_ago(2),
    catchup=False,
) as dag_serverless_batch:
    # [START how_to_cloud_dataproc_create_batch_operator]
    create_serverless_batch1 = DataprocCreateBatchOperator(
        task_id="customer_churn_etl_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG1,
        batch_id=BATCH_ID)
    create_serverless_batch2 = DataprocCreateBatchOperator(
        task_id="customer_churn_model_building_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG2,
        batch_id=BATCH_ID)
    create_serverless_batch3 = DataprocCreateBatchOperator(
        task_id="customer_churn_model_testing_serverless",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG3,
        batch_id=BATCH_ID
    )
    # [END how_to_cloud_dataproc_create_batch_operator]

    create_serverless_batch1 >> create_serverless_batch2 >> create_serverless_batch3
