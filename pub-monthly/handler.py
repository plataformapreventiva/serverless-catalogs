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
endpoint = '/_bulk'
elastic_address = os.getenv('HOST_ES')
elastic_index = os.getenv('INDEX_ES_PUB-MONTHLY')
elastic_type = "catalogo"

indexDoc = {
    "mappings": {
    "catalogo": {
    "properties": {
        "anio": {"type": "integer"},
        "numespago": {"type": "integer"},
        "cveent": {"type": "text"},
        "cvemuni": {"type": "text"},
        "origen": {"type": "text"},
        "cddependencia": {"type": "text"},
        "nbdependencia": {"type": "text"},
        "cdprograma": {"type": "text"},
        "nbprograma": {"type": "text"}
        }}},
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0}
    }


def create_catalog(s3r):
    athena_client = boto3.client('athena',
                                 region_name = 'us-west-2')
    b = s3r.Bucket('serverlesspub')
    objects_to_delete = list(b.objects.filter(Prefix="pub-monthly-catalog"))

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
              GROUP BY anio, numespago, cveent, cvemuni, origen, cddependencia,
                      nbdependencia, cdprograma, nbprograma""")

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'athena_pub'
        },
        ResultConfiguration={
            'OutputLocation':
            's3://serverlesspub/pub-monthly-catalog/',
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

def catalog(event, context):
    """
    Lambda Function to update pub catalog index at s3 event
    """

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    try:
        # define arguments
        max_rows_disp = "all"
        max_rows= None
        count = 0
        headers = []
        headers_position = {}
        to_elastic_string = ""
        datetime_field = ""
        id_column = None
        # get new db
        response = s3.get_object(Bucket=bucket, Key=key)
        lista = response['Body'].read().split(b'\n')
        headers_post = {"Content-type": "application/json", "Accept": "text/plain"}

        # Data sctructure
        keys =list(indexDoc["mappings"]["catalogo"]['properties'].keys())
        values = [ "%"+str(x)+"%" for x in keys]
        json_struct = str(dict(zip(keys, values))).replace("%',", "%',\n")

        connection = http.client.HTTPConnection(elastic_address)
        # Remove old index
        response = requests.delete("http://" + elastic_address + "/" + elastic_index)
        print("Returned from delete request: ", response)
        # Create index
        response = requests.put("http://" + elastic_address + "/" + elastic_index, data=json.dumps(indexDoc))
        print("Returned from create: ", response)

        for row in lista:
            row =  row.decode('utf-8').replace('"','')
            row = row.replace("'[","[").replace("]'","]")
            row = row.replace("}'", '}')# .replace("''","'")
            row = row.split("|")

            if count == 0:
                for iterator, col in enumerate(row):
                    headers.append(col)
                    headers_position[col] = iterator
            elif max_rows is not None and count >= max_rows:
                print('Max rows imported - exit')
                break
            elif len(row[0]) == 0:    # Empty rows on the end of document
                print("Found empty rows at the end of document")
                break
            else:
                pos = 0

                if os.name == 'nt':
                    _data = json_struct.replace("^", '"')
                else:
                    _data = json_struct.replace("'", '"')
                _data = _data.replace('\n','').replace('\r','')

                for header in headers:
                    if header == datetime_field:
                        datetime_type = dateutil.parser.parse(row[pos])
                        _data = _data.replace('"%' + header + '%"', '"{:%Y-%m-%dT%H:%M}"'.format(datetime_type))
                    else:
                        try:
                            if indexDoc["mappings"]["catalogo"]["properties"][headers[pos]]["type"] == 'integer':
                                _data = _data.replace('"%' + header + '%"', row[pos])
                            else:
                                _data = _data.replace('%' + header + '%', row[pos])
                        except:
                            _data = _data.replace('"%' + header + '%"',
                                str(yaml.load(row[pos])))
                    pos += 1

                if id_column is not None:
                    index_row = {"index": {"_index": elastic_index,
                                           "_type": elastic_type,
                                           '_id': row[headers_position[id_column]]}}
                else:
                    index_row = {"index": {"_index": elastic_index, "_type": elastic_type}}

                json_string = json.dumps(index_row) + "\n" + _data + "\n"
                json_string = json_string.replace("'null'",'null').replace('None',"null")
                json_string = json_string.replace('"[','[').replace(']"',"]")

                to_elastic_string = json_string.replace("'",'"').encode('utf-8')
                full_json_string += to_elastic_string
                connection = http.client.HTTPConnection(elastic_address)
                connection.request('POST', url=endpoint, headers = headers_post, body=to_elastic_string)
                response = connection.getresponse()
                print("Returned status code: ", response.status)
                print("Returned status text ", response.read())

            print('Iteración: ' + str(count))
            count += 1

        # Save json result
        today = date.today()
        s3.put_object(Body= full_json_string,
                Bucket = "serverlesspub",
                Key = "elastic/pub-monthly-catalog.temporal_{}".format(today))

    except Exception as e:
        raise e


