# universal-data-lake-emr-snowflake-athena-spark
universal-data-lake-emr-snowflake-athena-spark

Step by Step instruction 
https://www.linkedin.com/pulse/building-universal-data-lake-emr-serverless-hands-on-labs-soumil-shah-vftge/?trackingId=hhhWWFaZRjCCrNZ%2FDgTTPQ%3D%3D

# Code snipptes 


## Step 1: Download jar and sampel dataset upload to S3 
*  https://drive.google.com/drive/folders/1mQzZSVgxQGksoXkYGb8DSn2jeR48VehS?usp=share_link

## Step 2: Create EMR Serverless Cluster and Submit job 
```
export APPLICATION_ID="XXX"
export BUCKET_HUDI="soumilshah-dev-1995"
export IAM_ROLE="arn:aws:iam::XXXX:role/EMRServerlessS3RuntimeRole"

```

# Python job
```
import os
import time
import uuid
import json
import boto3


def check_job_status(client, run_id, applicationId):
    response = client.get_job_run(applicationId=applicationId, jobRunId=run_id)
    return response['jobRun']['state']


def lambda_handler(event, context):
    try:
        # Create EMR serverless client object
        client = boto3.client("emr-serverless")

        # Extracting parameters from the event
        jar = event.get("jar", [])
        # Add --conf spark.jars with comma-separated values from the jar object
        spark_submit_parameters = ' '.join(event.get("spark_submit_parameters", []))  # Convert list to string
        spark_submit_parameters = f'--conf spark.jars={",".join(jar)} {spark_submit_parameters}'  # Join with existing parameters

        arguments = event.get("arguments", {})
        job = event.get("job", {})

        # Extracting job details
        JobName = job.get("job_name")
        ApplicationId = job.get("ApplicationId")
        ExecutionTime = job.get("ExecutionTime")
        ExecutionArn = job.get("ExecutionArn")

        # Processing arguments
        entryPointArguments = []
        for key, value in arguments.items():
            if key == "hoodie-conf":

                # Extract hoodie-conf key-value pairs and add to entryPointArguments
                for hoodie_key, hoodie_value in value.items():
                    entryPointArguments.extend(["--hoodie-conf", f"{hoodie_key}={hoodie_value}"])
            elif isinstance(value, bool):
                # Add boolean parameters without values if True
                if value:
                    entryPointArguments.append(f"--{key}")
            else:
                entryPointArguments.extend([f"--{key}", f"{value}"])

        # Starting the EMR job run
        response = client.start_job_run(
            applicationId=ApplicationId,
            clientToken=str(uuid.uuid4()),
            executionRoleArn=ExecutionArn,
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': "command-runner.jar",
                    'entryPointArguments': entryPointArguments,
                    'sparkSubmitParameters': spark_submit_parameters
                },
            },
            executionTimeoutMinutes=ExecutionTime,
            name=JobName
        )

        if job.get("JobStatusPolling") == True:
            # Polling for job status
            run_id = response['jobRunId']
            print("Job run ID:", run_id)

            polling_interval = 3
            while True:
                status = check_job_status(client=client, run_id=run_id, applicationId=ApplicationId)
                print("Job status:", status)
                if status in ["CANCELLED", "FAILED", "SUCCESS"]:
                    break
                time.sleep(polling_interval)  # Poll every 3 seconds

        return {
            "statusCode": 200,
            "body": json.dumps(response)
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


# Test event


event = {
    "jar": [
        "/usr/lib/hudi/hudi-utilities-bundle.jar",
        "s3://soumilshah-dev-1995/jars/hudi-utilities-slim-bundle_2.12-0.14.0.jar",
        "s3://soumilshah-dev-1995/jars/hudi-extensions-0.1.0-SNAPSHOT-bundled.jar",
        "s3://soumilshah-dev-1995/jars/hudi-java-client-0.14.0.jar"
    ],
    "spark_submit_parameters": [
        "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
        "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        "--conf spark.sql.hive.convertMetastoreParquet=false",
        "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "--class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer"
    ],
    "arguments": {
        "table-type": "COPY_ON_WRITE",
        "op": "UPSERT",
        "enable-sync": True,
        "sync-tool-classes": "io.onetable.hudi.sync.OneTableSyncTool",
        "source-ordering-field": "replicadmstimestamp",
        "source-class": "org.apache.hudi.utilities.sources.ParquetDFSSource",
        "target-table": "invoice",
        "target-base-path": "s3://soumilshah-dev-1995/silver/invoice/",
        "hoodie-conf": {
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
            "hoodie.datasource.write.recordkey.field": "invoiceid",
            "hoodie.datasource.write.partitionpath.field": "destinationstate",
            "hoodie.deltastreamer.source.dfs.root": "s3://soumilshah-dev-1995/raw/parquet/",
            "hoodie.datasource.write.precombine.field": "replicadmstimestamp",
            "hoodie.onetable.formats.to.sync": "ICEBERG",
            "hoodie.onetable.target.metadata.retention.hr": 300
        }
    },
    "job": {
        "job_name": "delta_streamer_invoice",
        "created_by": "Soumil Shah",
        "created_at": "2024-03-20",
        "ApplicationId": os.getenv("APPLICATION_ID"),
        "ExecutionTime": 600,
        "JobActive": True,
        "schedule": "0 8 * * *",
        "JobStatusPolling": True,
        "JobDescription": "Ingest data from parquet source",
        "ExecutionArn": os.getenv("IAM_ROLE"),
    }
}

print(json.dumps(event, indent=3))
lambda_handler(event=event, context=None)

```

## Step 3: Create Glue crawlers and Glue DB
```
export IAM_ROLE_GLUE="arn:aws:iam::XX:role/service-role/AWSGlueServiceRole-test"
aws glue create-database --database-input "{\"Name\":\"icebergdb\"}"
aws glue get-databases

aws glue create-crawler \
    --name iceberg-invoices-crawler \
    --role $IAM_ROLE_GLUE \
    --database-name icebergdb \
    --targets '{"IcebergTargets": [{"Paths": ["s3://soumilshah-dev-1995/silver/invoice/"]}]}'

aws glue start-crawler --name iceberg-invoices-crawler
```

# Snowflake 
```

-- Step 1: Create a new database named TEMPDB
CREATE DATABASE TEMPDB;

-- Step 2: Switch to the newly created TEMPDB database
USE TEMPDB;

-- Step 3: Create an external volume in Snowflake pointing to an S3 bucket
CREATE OR REPLACE EXTERNAL VOLUME ext_vol
STORAGE_LOCATIONS = (
    (
        NAME = 'my-s3-us-east-1',  -- The name of the S3 storage location
        STORAGE_PROVIDER = 'S3',  -- Specifies that the storage provider is S3
        STORAGE_BASE_URL = 's3://soumilshah-dev-1995/silver/',  -- The base URL of the S3 bucket
        STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXX:role/snowflakes_role',  -- The AWS IAM role Snowflake will assume
        STORAGE_AWS_EXTERNAL_ID = 'ext_vol'  -- External ID used for the trust relationship between Snowflake and AWS
    )
);

-- Step 4: Retrieve the IAM user ARN for updating the trust policy in AWS
DESC EXTERNAL VOLUME snowflake_external_vol;

--  STRING JSON COPY : STORAGE_AWS_IAM_USER_ARN VALUES in Notepad
-- value is STORAGE_AWS_IAM_USER_ARN = arn:aws:iam::XXXX:user/e37q0000-s
DROP CATALOG INTEGRATION sf_glue_catalog_integ;

CREATE CATALOG INTEGRATION sf_glue_catalog_integ
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='icebergdb'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::XXX:role/snow_flake_glue_access'
  GLUE_CATALOG_ID='XX'
  GLUE_REGION='us-east-1'
  ENABLED=TRUE;


DESCRIBE CATALOG INTEGRATION sf_glue_catalog_integ

-- COPY GLUE_AWS_IAM_USER_ARN and GLUE_AWS_EXTERNAL_ID
-- GLUE_AWS_IAM_USER_ARN XXXXXX
-- GLUE_AWS_EXTERNAL_ID XXXXXX
-- UPDATE IAM ROLE


SHOW TABLES;


CREATE OR REPLACE ICEBERG TABLE invoice
EXTERNAL_VOLUME='ext_vol'
CATALOG='sf_glue_catalog_integ'
CATALOG_TABLE_NAME='invoice'


SELECT * FROM invoice
```

![Untitled Diagram drawio](https://github.com/user-attachments/assets/b02b3d53-a150-49b5-a834-4bb09c4dab0c)



