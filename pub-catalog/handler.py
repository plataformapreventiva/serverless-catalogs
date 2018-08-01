#!/usr/bin/env python
import boto3
import time
import argparse
import csv
import dateutil.parser
from datetime import date
import io
import http.client
import json
import os
import urllib
import yaml

from botocore.vendored import requests
from io import BytesIO

s3r = boto3.resource('s3')

def create_catalog(s3r):
    path = 'pub-catalog'
    athena_client = boto3.client('athena',
                                 region_name = 'us-west-2')
    b = s3r.Bucket('serverless-pub')
    objects_to_delete = list(b.objects.filter(Prefix=path))

    for obj in objects_to_delete:
        obj.delete()

    query=("""SELECT anio,
                     numespago,
                     cveent,
                     cvemuni,
                     origen,
                     cddependencia,
                     nbdependencia,
                     cdprograma,
                     nbprograma
              FROM pub_public
              GROUP BY anio, cveent, cvemuni, origen, cddependencia,
                      nbdependencia, cdprograma, nbprograma""")

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'athena_pub'
        },
        ResultConfiguration={
            'OutputLocation':
            's3://serverless-pub/{0}/'.format(path),
        }
    )

    execution_id = response['QueryExecutionId']
    response_status = athena_client.batch_get_query_execution(
        QueryExecutionIds=[
            execution_id
        ]
    )

    status = response_status['QueryExecutions'][0]['Status']['State']
    while status == 'RUNNING':
        response_status = athena_client.batch_get_query_execution(
            QueryExecutionIds=[
                execution_id
            ]
        )
        status = response_status['QueryExecutions'][0]['Status']['State']
        time.sleep(5)


    if status == 'SUCCEEDED':
        return 'success'


