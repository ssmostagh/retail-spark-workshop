# Retail Forecast using Serverless Spark through Vertex AI managed notebooks


Following are the lab modules:

[1. Understanding Data](05_retail_forecast_vertex_workaround.md#1-understanding-data)<br>
[2. Solution Architecture](05_retail_forecast_vertex_workaround.md#2-solution-architecture)<br>
[3. Declaring the cloud shell variables](05_retail_forecast_vertex_workaround.md#3-declaring-the-cloud-shell-variables)<br>
[4. Execution](05_retail_forecast_vertex_workaround.md#4-execution)<br>
[5. Logging](05a_retail_forecast_vertex_workaround.md#5-logging)<br>

<br>

## 1. Understanding Data

The datasets used for this project are

1. [Aisles data](01-datasets/aisles.csv). <br>
2. [Departments data](01-datasets/departments.csv) . <br>
3. [Orders data](01-datasets/orders.csv). <br>
4. [Products data](01-datasets/products.csv). <br>
5. [Order_products__prior](01-datasets/order_products__prior.csv). <br>
6. [Order_products__train](01-datasets/order_products__train.csv). <br>


- Aisles: This table includes all aisles. It has a single primary key (aisle_id)
- Departments: This table includes all departments. It has a single primary key (department_id)
- Products: This table includes all products. It has a single primary key (product_id)
- Orders: This table includes all orders, namely prior, train, and test. It has single primary key (order_id).
- Order_products_train: This table includes training orders. It has a composite primary key (order_id and product_id)
						and indicates whether a product in an order is a reorder or not (through the reordered variable).
- Order_products_prior : This table includes prior orders. It has a composite primary key (order_id and product_id) and
						indicates whether a product in an order is a reorder or not (through the reordered variable).

<br>

## 2. Solution Architecture


<kbd>
<img src=/lab-03/images/Flow_of_Resources.png />
</kbd>

<br>
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
SESSION_NAME=                                       # Serverless Session name.
UMSA_NAME=                                          #user managed service account required for the PySpark job executions
SERVICE_ACCOUNT=$UMSA_NAME@$PROJECT_ID.iam.gserviceaccount.com
NAME=                                               #Your unique identifier
```

**Note:** For all the variables except 'NAME', please ensure to use the values provided by the admin team.

<br>

### 3.4 Update Cloud Shell SDK version

Run the below on cloud shell-

```
gcloud components update

```

## 4. Execution


### 4.1. Run the Batch by creating sessions.

Run the below on cloud shell to create session. -
```
  gcloud beta dataproc sessions create spark $SESSION_NAME  \
--project=${PROJECT_ID} \
--location=${REGION} \
--property spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.2 \
--history-server-cluster=projects/$PROJECT_ID/regions/$REGION/clusters/$HISTORY_SERVER_NAME \
--subnet=$SUBNET \
--property=jupyter.notebook.gcsDir=$BUCKET_CODE

```
<br>

* Once the session is running navigate to Vertex AI

* Next, using the browser option from JupyterLab, navigate to the Notebook file located at: <br>
    <bucket_name> > 'retail_forecast_vertex_ai' > 00-scripts > retail-forecast.ipynb
* From the kernel dropdown list, select the kernel for the session created in section 3.2
* Pass the values to the variables project_name, dataset_name, bucket_name as provided by the Admin and replace user_name by your username
* Next, hit the **Execute** button as shown below to run the code in the notebook.

<br>

<kbd>
<img src=/lab-03/images/session10.png />
</kbd>


### 4.2. Check the output table in BQ

Navigate to BigQuery Console, and check the **retail_forecast** dataset. <br>
Once the data preparation batch is completed, four new tables '<your_name_here>_train_data', '<your_name_here>_test_data', '<your_name_here>_predictions_data' and '<your_name_here>_eval_output' will be created:

To view the data in these tables -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.

```
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_train_data` LIMIT 1000
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_test_data` LIMIT 1000
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_predictions_data` LIMIT 1000
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_eval_output` LIMIT 1000
```

**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively

<kbd>
<img src=/lab-03/images/image1.png />
</kbd>

<br>

## 5. Logging


### 5.1 Persistent History Server logs

To view the Persistent History server logs, click the 'View History Server' button on the Sessions monitoring page and the logs will be shown as below:

As the session is still in active state , we will be able to find the logs in show incomplete applications.

<br>

<kbd>
<img src=/lab-03/images/phs1.png />
</kbd>

<kbd>
<img src=/lab-03/images/image13_1.PNG />
</kbd>

<kbd>
<img src=/lab-03/images/image13.PNG />
</kbd>

<br>
