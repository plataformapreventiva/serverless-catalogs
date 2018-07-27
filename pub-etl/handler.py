#!/usr/bin/env python
import boto3
import argparse
import csv
import dateutil.parser
import io
import http.client
import json
import os
import urllib
import yaml

from botocore.vendored import requests
from io import BytesIO
from boto3.s3.transfer import S3Transfer

def emr_clean(event, context):
	# Get bucket and keGy from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
	# parse year
    year = (re.findall('\d{4}', key))[0]

	# Read emr config file
    with open('emr_config.yaml', 'r') as f:
        config = yaml.load(f)

    # script and bucket name for etl
    script_bucket_name = 'serverlesspub'
    script_name = 'scripts/pub-clean-sql.py'
    filename = 'pub-etl/pyspark/pub-clean-sql.py'

    # upload script to s3
    s3_conn = boto3.client('s3')
    transfer = S3Transfer(s3_conn)
    transfer.upload_file(filename,
                         script_bucket_name,
                         script_name)

    # EMR connection and cluster
    conn = boto3.client("emr")
    cluster_id = conn.run_job_flow(
        Name=config['cluster_name'],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=config['log_uri'],
        ReleaseLabel=config['software_version'],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'MasterInstanceType',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': config['master_instance_type'],
                    'InstanceCount': 1,
                },
                {
                    'Name': 'CoreInstanceType',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': config['slave_instance_type'],
                    'InstanceCount': config['instance_count'],
                }
            ],
            'Ec2KeyName': config['key_name'],
            'Ec2SubnetId': config['subnet_id'],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Applications=[{
            'Name': 'Spark'
        }],
        Configurations=[
            {
            "Classification":"spark-env",
            "Configurations":[{
                "Classification":"export",
                "Properties":{
                    "PYSPARK_PYTHON":"/usr/bin/python3",
                    "PYSPARK_DRIVER_PYTHON":"/usr/bin/python3"
                }
            }]
            },
            {
            "Classification": "capacity-scheduler",
            "Properties": {
                "yarn.scheduler.capacity.resource-calculator":
                    "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
            },
            {
            "Classification": "spark",
            "Properties": {
                "maximizeResourceAllocation": "true"
                }
            },
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.dynamicAllocation.enabled": "true",
                    }
            }
        ],
        Tags=[
            {
                'Key': 'pub'
                'Value': 'pub_sedesol'
            }
        ],
        Steps=[
            {
                'Name': 'setup - copy pub-clean-sql',
                'ActionOnFailure': 'CANCEL_AND_WAIT',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', '{0}{1}'.format(script_bucket_name, script_name),
                        '/home/hadoop/{0}'.format(script_name)]
                    }
            },
            {
                'Name': 'pub-clean',
                'ActionOnFailure': 'TERMINATE_CLUSTER', #'CANCEL_AND_WAIT'
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit',
                                '--conf', 'spark.executor.instances=4',
                                '--conf', 'spark.memory.fraction=0.6',
                                '--conf', 'spark.memory.storageFraction=0.5',
                                '--conf', 'spark.yarn.executor.memoryOverhead=1024',
                                '--conf', 'spark.yarn.driver.memoryOverhead=512',
                                '--conf', 'spark.executor.memory=1g',
                                '--conf', 'spark.driver.memory=1g',
                                '--conf', 'spark.driver.cores=1',
                                '--conf', 'spark.executor.cores=1',
                                '--conf', 'spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3',
                                '--conf', 'spark.executorEnv.PYSPARK_PYTHON=python3',
                                '/home/hadoop/{}'.format(script_name),
                                        '--year', year]

            }
            ],
    )
    return "Started cluster {}".format(cluster_id)
