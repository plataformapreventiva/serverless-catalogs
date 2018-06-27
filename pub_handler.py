#!/usr/bin/env python3

import boto3
import io
import json
import urllib
import csv
from io import BytesIO
import argparse
import http.client
import os
import csv
import json
import dateutil.parser

from botocore.vendored import requests


s3 = boto3.client('s3')
endpoint = '/_bulk'
elastic_address = "search-pp-search-pjomldcgauebmkfa26wkjfiv4y.us-west-2.es.amazonaws.com"
elastic_index = "cuaps-pub"
elastic_type = "catalogo"

indexDoc = {
    "dataRecord": {
        "properties": {
            "cd_dependencia" : {"type": "string"},
            "cd_programa" : {"type": "string"},
            "cd_padron" : {"type": "string"},
            "anio" : {"type": "string"},
            "iduni" : {"type": "string"},
            "nb_dependencia" : {"type": "string"},
            "nb_subp1" : {"type": "string"},
            "nb_subp2" : {"type": "string"},
            "nb_origen" : {"type": "string"},
            "nb_programa" : {"type": "string"},
            "cd_origen" : {"type": "string"},
            "cd_cuaps_x" : {"type": "string"},
            "cuaps_folio" : {"type": "string"},
            "cd_cuaps_y" : {"type": "string"},
            "chr_clave_presupuestal_pro" : {"type": "string"},
            "orden_gob" : {"type": "string"},
            "derechos_sociales" : {"type": "string"},
            "tipos_apoyos" : {"type": "string"}
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
    }

def CatalogPub(event, context):
    """Lambda Function to update pub catalog index from event.
    """

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    try:
        max_rows_disp = "all"
        max_rows= None
        count = 0
        headers = []
        headers_position = {}
        to_elastic_string = ""
        datetime_field = ""
        id_column = None
        response = s3.get_object(Bucket=bucket, Key=key)
        lista = response['Body'].read().split(b'\n')
        # TODO() Check body structure
        # remove old index
        connection = http.client.HTTPConnection(elastic_address)
        connection.request('XDELETE', url='/cuaps-pub')
        if lista[-1] == b'':
            lista = lista[:-1]
        json_struct = '{"cd_dependencia" : "%cd_dependencia%",\n  "cd_programa" : "%cd_programa%",\n  "cd_padron" : "%cd_padron%",\n  "anio" : "%anio%",\n  "iduni" : "%iduni%",\n  "nb_dependencia" : "%nb_dependencia%",\n "nb_subp1" : "%nb_subp1%",\n  "nb_subp2" : "%nb_subp2%",\n  "nb_origen" : "%nb_origen%",\n  "nb_programa" : "%nb_programa%",\n  "cd_origen" : "%cd_origen%",\n  "cuaps_folio" : "%cuaps_folio%",\n  "chr_clave_presupuestal_pro" : "%chr_clave_presupuestal_pro%",\n "derechos_sociales" : "%derechos_sociales%",\n  "tipos_apoyos" : "%tipos_apoyos%"}'

        for row in lista:
            row = row.decode('utf-8')
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
                        if  indexDoc["dataRecord"]["properties"][headers[pos]]["type"] == 'int':
                            _data = _data.replace('"%' + header + '%"', row[pos])
                        else:
                            _data = _data.replace('%' + header + '%',
                                    str(row[pos]).replace('"',""))
                    pos += 1
                if id_column is not None:
                    index_row = {"index": {"_index": elastic_index,
                                           "_type": elastic_type,
                                           '_id': row[headers_position[id_column]]}}
                else:
                    index_row = {"index": {"_index": elastic_index, "_type": elastic_type}}
                json_string = json.dumps(index_row) + "\n" + _data + "\n"
                to_elastic_string += json_string
            count += 1
        to_elastic_string = to_elastic_string.encode('utf-8')
        connection = http.client.HTTPConnection(elastic_address)
        headers_post = {"Content-type": "application/json", "Accept": "text/plain"}
        connection.request('POST', url=endpoint, headers = headers_post, body=to_elastic_string)
        response = connection.getresponse()
        print("Returned status code:", response.status)
        print("Returned status text", response.read())
        s3.put_object(Body= to_elastic_string,
                Bucket = "publicaciones-sedesol",
                Key = "catalogo_cuaps_pub.temporal")

    except Exception as e:

        raise e
