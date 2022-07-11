# Retail store analytics using Serverless Spark and Metastore with Google BigQuery

Following are the lab modules:

[1. Understanding Data](06-retail-store-analytics-bigquery-execution.md#1-understanding-the-data)<br>
[2. Solution Architecture](06-retail-store-analytics-bigquery-execution.md#2-solution-architecture)<br>
[3. Declaring Variables](06-retail-store-analytics-bigquery-execution.md#3-declaring-variables)<br>
[4. Running the job on BigQuery](06-retail-store-analytics-bigquery-execution.md#4-running-the-job-on-bigquery)<br>
[5. BQ output tables](06-retail-store-analytics-bigquery-execution.md#5-bq-output-tables)<br> 
[6. Logging](06-retail-store-analytics-bigquery-execution.md#6-logging)<br>

## 1. Understanding the data 

The datasets used for this project are 


1. [Aisles data](01-datasets/aisles/aisles.csv). <br>
2. [Departments data](01-datasets/departments/departments.csv) . <br>
3. [Orders data](01-datasets/orders/orders.csv). <br>
4. [Products data](01-datasets/products/products.csv). <br>
5. [Order_products__prior](01-datasets/order_products/order_products__prior.csv). <br>
6. [Order_products__train](01-datasets/order_products/order_products__train.csv). <br>


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
<img src= /lab-02/images/Flow_of_Resources.png>
</kbd>

<br>
<br>

**Data Pipeline**

The data pipeline involves the following steps: <br>
	- Create buckets in GCS <br>
	- Create Dataproc and Persistent History Server Cluster <br>
	- Copy the raw data files, pyspark and notebook files into GCS <br>
	- Create a metastore service in Cloud Dataproc <br>
	- Executing the code through Big Query <br>
	- Getting the output tables in Google BigQuery and dataproc <br>

<br>

## 3. Declaring Variables

#### 3.1 Set the PROJECT_ID in Cloud Shell

Open Cloud shell or navigate to [shell.cloud.google.com](https://shell.cloud.google.com)<br>
Run the below
```
gcloud config set project $PROJECT_ID

```

####  3.2 Verify the PROJECT_ID in Cloud Shell

Next, run the following command in cloud shell to ensure that the current project is set correctly:

```
gcloud config get-value project
```

####  3.3 Declare the variables

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
METASTORE_NAME                                      #name of the metastore which will store our schema
NAME=                                               #your name
```

### 3.4 Update Cloud Shell SDK version

Run the below on cloud shell-

```
gcloud components update

```

## 4. Running the job on BigQuery

### 4.1. Navigate to PySpark Console in BigQuery
    
Open BigQuery Console

Click on the the dropdown to "Compose new query" and click on **"Compose new PySpark"**
    
![this is a screenshot](/lab-02/images/bq_pyspark_console.png)

<br>

### 4.2. Provide Configuration for Serverless Spark job
    
Navigate to Bigquery > Compose new PySpark Query > PySpark Options

How to reach to configure a Serverless Spark job

Step-1
<br>
<kbd>
<img src=/lab-02/images/part1.PNG />
</kbd>
<br>

Step-2 
<br>

<kbd>
<img src=/lab-02/images/p1.PNG />
</kbd>

<br>

Next, fill in the following values in the PySpark creation window :



- **GCS Staging Folder**   - <your_bucket_name>

- **Region**     - The region name provided by the Admin team

- **Service Account**    - <UMSA_NAME>@<PROJECT_ID>.iam.gserviceaccount.com

<br>

<kbd>
<img src=/lab-02/images/pyspark-session-part1.PNG />
</kbd>

<br>

<br>

<kbd>
<img src=/lab-02/images/part2.PNG />
</kbd>

<br>

- **Arguments** - <br>
  Four Arguments needs to be provided: <br>
    *  <your_project_name>                                                                      #project name
    *  <your_bq_dataset_name>                                                                   #dataset_name
    * <your_code_bucket_name>
    * <your_name>

  **Note:** Press RETURN after each argument <br>
  **Note:** The arguments must be passed in the same order as mentioned as they are extracted in the order they are provided

- **Network Configuration** - select the network and subnetwork with Private Google Access Enabled

- **Hive Metastore** - select the Dataproc metastore created by the Admin

- **History Server Cluster** - projects/<PROJECT_ID>/regions/<REGION_NAME>/clusters/<HISTORY_SERVER_NAME>

<br>

<kbd>
<img src=/lab-02/images/part3.png />
</kbd>

<br>

<br>

<kbd>
<img src=/lab-02/images/part6.PNG />
</kbd>

<br>

<br>

<kbd>
<img src=/lab-02/images/part5.png />
</kbd>

<br>
Once all the details are in, you can save the session. Once the serverless spark session is created, you can execute your code against this session.

## 4.3. Submit the BigQuery PySpark Job

Copy the code 00-scripts/retail-store-analytics-bigquery.py in to the big query notebook created.
Next, hit the Run button as shown .


![this is a screenshot](/lab-02/images/bq2.png)

<br>
<br>

### 4.4. Examine the BigQuery Batch Details
    
Once you submit the job, you will the see the Batches page populate with the current run.

To navigate, click on **Last Batch Run**, which will open the page with the batch details and you can monitor the execution and output.

![this is a screenshot](/lab-02/images/bq3.png)
<br>
<br>

The same details are found on the BigQuery Output section as well.

![this is a screenshot](/lab-02/images/bq4.JPG)

<br>
<br>
    <hr>

Congratulations on successfully completing the first Serverless Spark Batch on BigQuery. As you can see in the example, no cluster or infrastructure administration was required to perform this batch. Dataproc serverless is responsible for launching the cluster, running the job, scaling it as needed, and cleaning up after the job is completed.

## 5. BQ output tables

Navigate to BigQuery Console, and check the **<your_BQ_DATASET_NAME>** dataset. <br>
Once the Serverless batches execution is completed, a new new table '<your_name_here>_inventory_data' will be created:

<br>

<kbd>
<img src=/lab-02/images/bigqueryretail1.png />
</kbd>

<br>

To view the data in these tables -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

<kbd>
<img src=/lab-02/images/bq_preview.png />
</kbd>

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.

```
  SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_inventory_data` LIMIT 1000;


```
<br>

<kbd>
<img src=/lab-02/images/bq_3.PNG />
</kbd>

<br>

**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively

<br>

## 6. Logging

### 6.1 Serverless Batch logs

Logs associated with the application can be found in the logging console under
**Dataproc > Serverless > Batches > <batch_name>**.
<br> You can also click on “View Logs” button on the Dataproc batches monitoring page to get to the logging page for the specific Spark job.

<kbd>
<img src=/lab-02/images/image10.png />
</kbd>

<kbd>
<img src=/lab-02/images/image11.png />
</kbd>

<br>

### 6.2 Persistent History Server logs

To view the Persistent History server logs, click the 'View History Server' button on the Dataproc batches monitoring page and the logs will be shown as below:

<br>

<kbd>
<img src=/lab-02/images/image12.png />
</kbd>

<kbd>
<img src=/lab-02/images/image13.png />
</kbd>

<br>

### 6.3. Metastore logs

To view the metastore logs, click the 'View Logs' button on the metastore page and the logs will be shown as below:

<br>

<kbd>
<img src=/lab-02/images/meta_logs01.png />
</kbd>

<kbd>
<img src=/lab-02/images/meta_logs02.png />
</kbd>

<br>

