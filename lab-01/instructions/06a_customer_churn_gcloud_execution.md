# Customer Churn using Serverless Spark through Google Cloud Shell

**Goal** - Data Preparation and Model Training for Detecting Customer Churn.

Following are the lab modules:

[1. Understanding Data](instructions/06a_customer_churn_gcloud_execution.md#1-understanding-data)<br>
[2. Solution Architecture](instructions/06a_customer_churn_gcloud_execution.md#2-solution-architecture)<br>
[3. Declaring cloud shell Variables](instructions/06a_customer_churn_gcloud_execution.md#3-declaring-cloud-shell-variables)<br>
[4. Data Preparation](instructions/06a_customer_churn_gcloud_execution.md#4-data-preparation)<br>
[5. Model Training and Testing](instructions/06a_customer_churn_gcloud_execution.md#5-model-training-and-testing)<br>
[6. Model Evaluation](instructions/06a_customer_churn_gcloud_execution.md#6-model-evaluation)<br>
[7. Logging](instructions/06a_customer_churn_gcloud_execution.md#7-logging)<br>

<br>

## 1. Understanding Data

The dataset used for this project are [customer churn data](01-datasets/churn_dataset.csv) and [customer test data](01-datasets/test_model_data.csv). <br>

The dataset contains the following features:

- Churn - Binary field which represents customers who left/were retained within the last month
- Services that each customer has signed up for – phone, multiple lines, internet, online security, online backup, device protection, tech support, and streaming TV and movies
- Customer account information – how long they’ve been a customer, contract, payment method, paperless billing, monthly charges, and total charges
- Demographic info about customers – gender, age range, and if they have partners and dependents


**Note:** The following features refer to these same-host connections.

- serror_rate
- rerror_rate
- same_srv_rate
- diff_srv_rate
- srv_count

**Note:** The following features refer to these same-service connections.
- srv_serror_rate
- srv_rerror_rate
- srv_diff_host_rate

<br>

## 2. Solution Architecture

<kbd>
<img src=/lab-01/images/Flow_of_Resources.jpeg />
</kbd>

<br>
<br>

**Model Pipeline**

The model pipeline involves the following steps:
 - Data cleanup and preparation
 - Building and training two Machine Learning Models (Logistic Regression and Random Forest Classifier) before saving them into cloud storage
 - Using the model built in above step to evaluate test data

<br>

## 3. Declaring cloud shell variables

#### 3.1 Set the PROJECT_ID in Cloud Shell

Open Cloud shell or navigate to [shell.cloud.google.com](https://shell.cloud.google.com)<br>
Run the below
```
gcloud config set project $PROJECT_ID

```

#### 3.2 Verify the PROJECT_ID in Cloud Shell

Next, run the following command in cloud shell to ensure that the current project is set correctly:

```
gcloud config get-value project
```

#### 3.3 Declare the variables

Based on the prereqs and checklist, declare the following variables in cloud shell by replacing with your values:


```
PROJECT_ID=$(gcloud config get-value project)       #current GCP project where we are building our use case
REGION=                                             #GCP region where all our resources will be created
SUBNET=                                             #subnet which has private google access enabled
BUCKET_CODE=                                        #GCP bucket where our code, data and model files will be stored
BUCKET_PHS=                                         #bucket where our application logs created in the history server will be stored
HISTORY_SERVER_NAME=                                #name of the history server which will store our application logs
BQ_DATASET_NAME=                                    #BigQuery dataset where all the tables will be stored
UMSA=serverless-spark                               #name of the user managed service account required for the PySpark job executions
SERVICE_ACCOUNT=$UMSA@$PROJECT_ID.iam.gserviceaccount.com
NAME=                                               #Your unique identifier
```

**Note:** For all the variables except 'NAME', please ensure to use the values provided by the admin team.

### 3.4 Update Cloud Shell SDK version

Run the below on cloud shell-

```
gcloud components update

```
<br>

## 4. Data Preparation

Based on EDA, the data preparation script has been created. Among the 21 columns, relevant features have been selected and stored in BQ for the next step of model training.

### 4.1. Run PySpark Serverless Batch for Data Preparation

Run the below on cloud shell -
```
gcloud dataproc batches submit \
  --project $PROJECT_ID \
  --region $REGION \
  pyspark --batch ${NAME}-batch-${RANDOM} \
  gs://$BUCKET_CODE/customer_churn/00-scripts/customer_churn_data_prep.py \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar \
  --subnet $SUBNET \
  --service-account $SERVICE_ACCOUNT \
  --history-server-cluster projects/$PROJECT_ID/regions/$REGION/clusters/$HISTORY_SERVER_NAME \
  -- $PROJECT_ID $BQ_DATASET_NAME $BUCKET_CODE $NAME
```


### 4.2. Check the output table in BQ

Navigate to BigQuery Console, and check the **customer_churn_lab** dataset. <br>
Once the data preparation batch is completed, a new table '<your_name_here>_training_data' and '<your_name_here>_test_data' will be created.

To view the data in these tables -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

<kbd>
<img src=/retail-spark-workshop/lab-01/images/bq_preview.png />
</kbd>

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.


To query the table -
```
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_training_data` LIMIT 1000;
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_test_data` LIMIT 1000;
```

**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively


<kbd>
<img src=retail-spark-workshop/lab-01/images/image5.png />
</kbd>



<br>

<br>

<br>

## 5. Model Training and Testing

### 5.1. Run PySpark Serverless Batch for Model Training and Testing

The following script will train the model and save the model in the bucket.

Use the gcloud command below:

```
gcloud dataproc batches submit \
  --project $PROJECT_ID \
  --region $REGION \
  pyspark --batch ${NAME}-batch-${RANDOM} \
  gs://$BUCKET_CODE/customer_churn/00-scripts/customer_churn_model_building.py \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar  \
  --subnet $SUBNET \
  --service-account $SERVICE_ACCOUNT \
  --history-server-cluster projects/$PROJECT_ID/regions/$REGION/clusters/$HISTORY_SERVER_NAME \
  -- $PROJECT_ID $BQ_DATASET_NAME $BUCKET_CODE $NAME
```

### 5.2. Query the model_test results BQ table

Navigate to BigQuery Console, and check the **customer_churn_lab** dataset. <br>
Once the modelling  batch is completed, a new table '<your_name_here>_predictions_data' will be created.

To view the data in this table -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

<kbd>
<img src=retail-spark-workshop/lab-01/images/bq_preview.png />
</kbd>

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.

```
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_predictions_data` LIMIT 1000;
```
**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively

<kbd>
<img src=retail-spark-workshop/lab-01/images/image6.png />
</kbd>


<br>
<br>
<br>

## 6. Model Evaluation

### 6.1. Run PySpark Serverless Batch for Model Evaluation

The following script will load the model and predict the new data.

Use the gcloud command below:

```
gcloud dataproc batches submit \
  --project $PROJECT_ID \
  --region $REGION \
  pyspark --batch ${NAME}-batch-${RANDOM} \
  gs://$BUCKET_CODE/customer_churn/00-scripts/customer_churn_model_testing.py \
  --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.22.2.jar  \
  --subnet $SUBNET \
  --service-account $SERVICE_ACCOUNT \
  --history-server-cluster projects/$PROJECT_ID/regions/$REGION/clusters/$HISTORY_SERVER_NAME \
  -- $PROJECT_ID $BQ_DATASET_NAME $BUCKET_CODE $NAME
```

### 6.2. Query the model_test results BQ table

Navigate to BigQuery Console, and check the **customer_churn_lab** dataset. <br>
Once the model_testing  batch is completed, a new table '<your_name_here>_test_output' will be created.

To view the data in this table -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

<kbd>
<img src=retail-spark-workshop/lab-01/images/bq_preview.png />
</kbd>

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.

```
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_test_output` LIMIT 1000;
```
**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively

<kbd>
<img src=retail-spark-workshop/lab-01/images/image7.png />
</kbd>


<br>
<br>
<br>

## 7. Logging

### 7.1 Serverless Batch logs

Logs associated with the application can be found in the logging console under
**Dataproc > Serverless > Batches > <batch_name>**.
<br> You can also click on “View Logs” button on the Dataproc batches monitoring page to get to the logging page for the specific Spark job.

<kbd>
<img src=retail-spark-workshop/lab-01/images/image8.png />
</kbd>

<kbd>
<img src=retail-spark-workshop/lab-01/images/image13.png />
</kbd>

<br>

### 7.2 Persistent History Server logs

To view the Persistent History server logs, click the 'View History Server' button on the Dataproc batches monitoring page and the logs will be shown as below:

<br>

<kbd>
<img src=retail-spark-workshop/lab-01/images/image17.png />
</kbd>

<kbd>
<img src=retail-spark-workshop/lab-01/images/image14.png />
</kbd>

<br>
