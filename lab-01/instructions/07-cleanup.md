# About

This module includes the cleanup of resources created for the lab.

[1. Declare variables](instructions/07-cleanup.md#1-declare-variables)<br>
[2. Delete Buckets](instructions/07-cleanup.md#2-delete-buckets)<br>
[3. Delete Spark Persistent History Server](instructions/07-cleanup.md#3-delete-spark-persistent-history-server)<br>
[4. Delete BQ Dataset](instructions/07-cleanup.md#4-delete-bq-dataset)<br>
[5. Delete Composer](instructions/07-cleanup.md#5-delete-composer)
                                   
## 0. Prerequisites 

#### 1. GCP Project Details
Note the project number and project ID. <br>
We will need this for the rest fo the lab


#### 2. Attach cloud shell to your project.
Open Cloud shell or navigate to [shell.cloud.google.com](https://shell.cloud.google.com) <br>
Run the below command to set the project in the cloud shell terminal:
```
gcloud config set project $PROJECT_ID

```

<br>

## 1. Declare variables 

We will use these throughout the lab. <br>
Run the below in cloud shells coped to the project you selected-

```
PROJECT_ID= #Project ID
REGION= #Region to be used
BUCKET_PHS= #Bucket name for Persistent History Server
BUCKET_CODE =  #GCP bucket where our code, data and model files will be stored
BQ_DATASET_NAME = #BigQuery dataset where all the tables will be stored
PHS_NAME = # Spark Persistent History Server name
```

<br>

## 2. Delete buckets

Follow the commands to delete the following buckets 
1. Bucket attached to spark history server
2. Bucket with code files

```
gcloud alpha storage rm --recursive gs://$BUCKET_PHS
gcloud alpha storage rm --recursive gs://$BUCKET_CODE
```

<br>

## 3. Delete Spark Persistent History Server

Run the below command to delete Spark PHS

```
gcloud dataproc clusters delete $PHS_NAME \
  --region=${REGION} 
```

<br>

## 4. Delete BQ Dataset

Run the below command to delete BQ dataset and all the tables within the dataset

```
gcloud alpha bq datasets delete $BQ_DATASET_NAME \
--remove-tables
```

## 5. Delete Composer


step 1:Run the below command to delete composer environment

```
gcloud composer environments delete ENVIRONMENT_NAME \
--location LOCATION
```
step 2: Delete the persistent disk of your environment's Redis queue. Deleting the Cloud Composer environment does not delete its persistent disk.
To delete your environment's persistent disk:

```
gcloud compute disks delete PD_NAME \
--region=PD_LOCATION
```

PD_NAME --> name of the persistent disk for your environment
PD_LOCATION --> the location of the persistent disk. For example, the location can be [us-central1-a] .

