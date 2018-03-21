#!/usr/bin/env python3
import json
import requests

import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch

import es_utils

# TODO() .env domain
domain = "https://search-pp-search-pjomldcgauebmkfa26wkjfiv4y.us-west-2.es.amazonaws.com"
index_name = "pub-monthly-catalog_2"
doc_type = index_name
s3 = boto3.client('s3')

# Define Catalog Schema
indexDoc = {
    "dataRecord" : {
        "properties" : {
            "anio" : {
                "type" : "int"
                },
            "cvemuni" : {
                "type" : "string"
                },
            "nbdependencia" : {
                "type" : "string"
                },
            "cddependencia" : {
                "type" : "int"
                },
            "cdprograma" : {
                "type" : "string"
                },
            "numespago" : {
                "type" : "int"
                },
            "cveent" : {
                "type" : "string"
                },
            "nbprograma" : {
                "type" : "string"
                },
            "origin" : {
                "type" : "string"
                }
        }
    },
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
}


def lambda_handler(event, context):
    """Lambda Function to update pub catalog index from event.
	"""
	print("Received event: " + json.dumps(event, indent=2))
    esClient = es_utils.connect_es(domain)

    # TODO() - Check and update only the last month.
    es_utils.create_index(esClient, index_name, indexDoc, replace=True) # False -> update

    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
		# Pandarise the catalog csv from object and read schema
        data = pd.read_csv(io.BytesIO(response['Body'].read()), dtype={'anio': 'int',
                                                                       #'numespago':'int',
                                                                       'cveent':'str',
                                                                       'cvemuni':'str'})
                                                                       #'cddependencia':'int'})

        try:
            index_pandas_to_es(esClient, index_name, doc_type, replace=True)
        except:
            index_pandas_to_es_by_chuncks(data,index=index_name,doc_type=doc_type,
                                          chunk_size=10000)

	except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. \
			  Make sure they exist and your bucket is in the \
			  same region as this function.'.format(key, bucket))
        raise e
