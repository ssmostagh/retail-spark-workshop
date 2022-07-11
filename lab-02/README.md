drive.web-frontend_20220629.00_p0
# Retail store analytics 


## 1. Overview

With the advent of cloud environments, the concept of huge capital investments in infrastructure in terms of capital and maintenance is a thing of the past. Even when it comes to provisioning infrastructure on cloud services, it can get tedious and cumbersome.
In this example, you will look at executing a simple PySpark code which runs on Serverless batch (a fully managed Dataproc cluster). It is similar to executing code on a Dataproc cluster without the need to initialize, deploy or manage the underlying infrastructure.
This usecase deals with the analysis of retail store data.
<br> 

## 2. Services Used

* Google Cloud Storage
* Google Cloud Dataproc
* Google Cloud BigQuery

<br>

## 3. Permissions / IAM Roles required to run the lab

Following permissions / roles are required to execute the serverless batch

- Viewer
- Dataproc Editor
- BigQuery Data Editor
- Service Account User
- Storage Admin

<br>

## 4. Checklist

To perform the lab, below are the list of activities to perform.-<br>

[1. GCP Prerequisites ](/instructions/01-gcp-prerequisites.md) <BR>
[2. Spark History Server Setup](/instructions/02-persistent-history-server.md) <BR>
[3. Creating a GCS Bucket](/instructions/03-files-upload.md ) <BR>
[4. Creating a BigQuery Dataset](/instructions/04-bigquery-dataset.md) <BR>
[5. Metastore Creation](/instructions/05-metastore-creation.md)

Note down the values for below variables to get started with the lab:

```
PROJECT_ID=                                         #Current GCP project where we are building our use case
REGION=                                             #GCP region where all our resources will be created
SUBNET=                                             #subnet which has private google access enabled
BQ_DATASET_NAME=                                    #BigQuery dataset where all the tables will be stored
BUCKET_CODE=                                        #GCP bucket where our code, data and model files will be stored
BUCKET_PHS=                                         #bucket where our application logs created in the history server will be stored
HISTORY_SERVER_NAME=                                #name of the history server which will store our application logs
UMSA_NAME=                                          #user managed service account required for the PySpark job executions
SERVICE_ACCOUNT=$UMSA_NAME@$PROJECT_ID.iam.gserviceaccount.com
NAME=<your_name_here>                               #Your Unique Identifier
```
<br>

***Note: The region to submit serverless spark job, VPC Subnet and Staging bucket should be same.***

<br>

## 5. Lab Modules

The lab consists of the following modules.
 - Understand the Data
 - Solution Architecture
 - Executing ETL
 - Examine the logs
 - Explore the output

<br>

There is 1 way to perform the lab
    - Using [GCP sessions through Big Query](instructions/06-retail-store-analytics-bigquery-execution.md)
    
## 6. CleanUp  

Delete the resources after finishing the lab. 

Refer - [Cleanup](instructions/07-cleanup.md )
