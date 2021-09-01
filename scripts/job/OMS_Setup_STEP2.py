# Databricks notebook source
# MAGIC %md ## Define OMS Base Configurations

# COMMAND ----------

# MAGIC %md
# MAGIC #### The values for `omsBaseLocation` and `omsDBName` should match with the parameters used during DeltaOMS initialization. 
# MAGIC 
# MAGIC #### You can choose any valid value for `omsCheckpointSuffix` and `omsCheckpointBase` as appropriate for your environment

# COMMAND ----------

omsBaseLocation = "dbfs:/user/hive/warehouse/oms"
omsDBName = "oms_test_aug31"
omsCheckpointSuffix = "_aug31_171000"
omsCheckpointBase = f"{omsBaseLocation}/_checkpoints"
print(omsBaseLocation,omsDBName,omsCheckpointBase,omsCheckpointSuffix)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Databricks Job Creation Template

# COMMAND ----------

import json
# Must use DBR version 8.3+
dbr_version = '8.3.x-scala2.12'
# Put appropriate Instance Profile ARN
instance_profile_arn = 'INSTANCE_PROFILE_ARN'
# Put appropriate ZoneId
zone_id = 'us-west-2a'
# Put appropriate Node Type
node_type_id = 'i3.xlarge'
policy_id = 'POLICY_ID_FOR_YOUR_ENVIRONMENT/CLUSTER'
oms_jar_location = 'VALID LOCATION FOR THE DELTA OMS JAR'
oms_ingest_main_class_name = 'com.databricks.labs.deltaoms.ingest.StreamPopulateOMS'

job_create_template = {
        "new_cluster": {
            "spark_version": f"{dbr_version}",
            "aws_attributes": {
                "availability": "SPOT_WITH_FALLBACK",
                "instance_profile_arn": f"{instance_profile_arn}",
                "first_on_demand": 1,
                "zone_id": f"{zone_id}"
            },
            "node_type_id": f"{node_type_id}",
            "driver_node_type_id": f"{node_type_id}",
            "cluster_log_conf": {
                "dbfs": {
                    "destination": "dbfs:/cluster-logs/oms/"
                }
            },
            "spark_env_vars": {
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            "init_scripts": [
                {
                    "dbfs": {
                        "destination": "dbfs:/init_scripts/oms/update_log4j_properties.sh"
                    }
                }
            ],
            "policy_id": f"{policy_id}",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 3
            }
        },
        "libraries": [
            {
                "jar": f"{oms_jar_location}"
            }
        ],
        "spark_jar_task": {
            "main_class_name": f"{oms_ingest_main_class_name}",
            "parameters": [
                f"--dbName={omsDBName}",
                f"--baseLocation={omsBaseLocation}",
                f"--checkpointBase={omsCheckpointBase}",
                f"--checkpointSuffix={omsCheckpointSuffix}",
                "--skipPathConfig",
                "--skipInitializeOMS"
            ]
        },
        "email_notifications": {},
        "max_concurrent_runs": 1
}

# COMMAND ----------

# MAGIC %md #### Simple Databricks REST API Python Client for Job Creation

# COMMAND ----------

import requests

DOMAIN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

def create_job(job_settings):
  response = requests.post(
    f'{DOMAIN}/api/2.0/jobs/create',
    headers={'Authorization': f'Bearer {TOKEN}'},
    json=job_settings
  )
  if response.status_code == 200:
    print(response.json()['job_id'])
  else:
    print(f'Error creating job : {response.json()["error_code"]}, {response.json()["message"]}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Databricks Jobs for Ingestion and Processing

# COMMAND ----------

import math
import copy
# Change DRY RUN to False for creating the job
DRY_RUN=True
# Find number of unique wildcard paths
num_wildcard_paths = spark.sql(f"select count(distinct wuid) as cwuid from {omsDBName}.pathconfig").head().cwuid
# Set number of streams per job (default is 50.0)
num_streams_per_job = 50.0
job_name_prefix = "OMS_Streaming_Ingestion"
num_of_jobs = int(math.ceil(num_wildcard_paths/num_streams_per_job))
print(f"Creating {num_of_jobs} Databricks Jobs for OMS using the Job Template")
ss = es = 0
for i in range(1,num_of_jobs+1):
  ss = es + 1
  es = ss + int(num_streams_per_job-1)
  job_name_dict = copy.deepcopy(job_create_template)
  job_name_dict['name'] = f"{job_name_prefix}_{ss}_{es}"
  job_name_dict['spark_jar_task']['parameters'].append(f"--startingStream={ss}")
  job_name_dict['spark_jar_task']['parameters'].append(f"--endingStream={es}")
  print("##############################################################")
  print(f"Creating Job {i} with json {job_name_dict}")
  if not DRY_RUN:
    create_job(job_name_dict)
  else:
    print("##############################################################")
    print("**************************************************************")
    print("THIS WAS A DRY RUN !! No actual Databricks Jobs were created")
    print("**************************************************************")

# COMMAND ----------


