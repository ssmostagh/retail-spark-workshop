# Customer Churn using Serverless Spark through Airflow.

**Goal** - Data Preparation and Model Training for Detecting Customer Churn Dataset.

Following are the lob modules:

[1. Understanding Data](instructions/06c_customer_churn_airflow_execution.md#1-understanding-data)<br>
[2. Solution Diagram](instructions/06c_customer_churn_airflow_execution.md#2-solution-diagram)<br>
[3. Uploading DAG files to DAGs folder](instructions/06c_customer_churn_airflow_execution.md#3-uploading-dag-files-to-dags-folder)<br>
[4. Execution of Airflow DAG](instructions/06c_customer_churn_airflow_execution.md#4-execution-of-airflow-dag)<br>
[5. BQ output tables](instructions/06c_customer_churn_airflow_execution.md#5-bq-output-tables)<br>
[6. Logging](instructions/06c_customer_churn_airflow_execution.md#6-logging)<br>

<br>

## 1. Understanding Data

The dataset used for this project are [customer churn data](01-datasets/customer_churn_train_data.csv) and [customer test data](01-datasets/customer_churn_test_data.csv). <br>
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

## 2. Solution Diagram

<kbd>
<img src=/lab-01/images/Flow_of_Resources.jpeg />
</kbd>

<br>
<br>

**Model Pipeline**

The model pipeline involves the following steps: <br>
	- Create buckets in GCS <br>
	- Create Dataproc and Persistent History Server Cluster <br>
	- Copy the raw data files, PySpark and notebook files into GCS <br>
	- Create a Cloud Composer environment and Airflow jobs to run the serverless spark job <br>
	- Creating Google BigQuery tables with summary of anomalous cell towers <br>

<br>

## 3. Uploading DAG files to DAGs folder

* From the code repository, download the file located at: **customer_churn**>**00-scripts**>**customer_churn_airflow.py**
* Rename file to <your_name_here>-customer_churn_airflow.py
* Open the file and replace your name on row 21
* Navigate to **Composer**>**<composer_environment>**
* Next, navigate to **Environment Configuration**>**DAGs folder URI**
* Next, upload the DAG file to the GCS bucket corresponding to the **DAGs folder URI**

<kbd>
<img src=/lab-01/images/composer_2.png />
</kbd>

<br>
<br>
<br>

<kbd>
<img src=/lab-01/images/composer_3.png />
</kbd>

<br>
<br>
<br>


## 4. Execution of Airflow DAG

* Navigate to **Composer**>**<your_environment>**>**Open Airflow UI**

<kbd>
<img src=/lab-01/images/composer_5.png />
</kbd>

<br>

* Once the Airflow UI opens, navigate to **DAGs** and open your respective DAG
* Next, trigger your DAG by clicking on the **Trigger DAG** button

<kbd>
<img src=/lab-01/images/composer_6.png />
</kbd>

<br>

* Once the DAG is triggered, the DAG can be monitored directly through the Airflow UI as well as the Dataproc>Serverless>Batches window

<kbd>
<img src=/lab-01/images/composer_7.PNG />
</kbd>

<br>

## 5. BQ output tables

Navigate to BigQuery Console, and check the **customer-churn** dataset. <br>
Once the Airflow DAG execution is completed, four new tables '<your_name_here>_training_data', '<your_name_here>_test_data', '<your_name_here>_predictions_data' and '<your_name_here>_test_output' are created.


To view the data in these tables -

* Select the table from BigQuery Explorer by navigating 'project_id' **>** 'dataset' **>** 'table_name'
* Click on the **Preview** button to see the data in the table

<br>

<kbd>
<img src=/lab-01/images/bq_preview.png />
</kbd>

<br>

**Note:** If the **Preview** button is not visible, run the below queries to view the data. However, these queries will be charged for the full table scan.


```
SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_training_data` LIMIT 1000;
SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_test_data` LIMIT 1000;
SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_predictions_data` LIMIT 1000;
SELECT * FROM `<project_name>.<dataset_name>.<your_name_here>_test_output` LIMIT 1000;

```


<kbd>
<img src=/lab-01/images/image5.png />
</kbd>

<br>

<kbd>
<img src=/lab-01/images/_test_data.png />
</kbd>


**Note:** Edit all occurrences of <project_name> and <dataset_name> to match the values of the variables PROJECT_ID, and BQ_DATASET_NAME respectively

<br>

<br>

## 6. Logging

### 6.1 Airflow logging

* To view the logs of any step of the DAG execution, click on the **<DAG step>**>**Log** button <br>

<kbd>
<img src=/lab-01/images/composer_8.png />
</kbd>

<br>

### 6.2 Serverless Batch logs

Logs associated with the application can be found in the logging console under
**Dataproc > Serverless > Batches > <batch_name>**.
<br> You can also click on “View Logs” button on the Dataproc batches monitoring page to get to the logging page for the specific Spark job.

<kbd>
<img src=/lab-01/images/image10.png />
</kbd>

<kbd>
<img src=/lab-01/images/image11.png />
</kbd>

<br>

### 6.3 Persistent History Server logs

To view the Persistent History server logs, click the 'View History Server' button on the Dataproc batches monitoring page and the logs will be shown as below:

<br>

<kbd>
<img src=/lab-01/images/image12.png />
</kbd>

<kbd>
<img src=/lab-01/images/image13.png />
</kbd>

<br>
