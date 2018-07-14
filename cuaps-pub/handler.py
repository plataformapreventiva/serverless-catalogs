#!/usr/bin/env python3

import argparse
import boto3
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

s3 = boto3.client('s3')
endpoint = '/_bulk'
elastic_address = os.getenv('HOST_ES')
elastic_index = os.getenv('INDEX_ES')
elastic_type = "catalogo"

indexDoc = {
        "mappings": {
        "catalogo": {
        "properties": {
            "cd_programa" : {"type": "text"},
            "id_pub" : {"type": "text"},
            "nb_programa" : {"type": "text"},
            "DGAE" : {"type": "text"},
            "DGAIP" : {"type": "text"},
            "anio" : {"type": "text"},
            "cuaps_folio" : {"type": "text"},
            "derechos_sociales" : {"type": "text"},
            "nb_origen" : {"type": "text"},
            "nb_subp1" : {"type": "text"},
            "origen" : {"type": "text"},
            "apoyos" :{
                    "properties":{
                        "indic_s" : {"type": "text"},
                        "cuenta_info_geo" : {"type": "text"},
                        "obj_compo" : {"type": "text"},
                        "indic_p" : {"type": "text"},
                        "otro_actualiza_padron" : {"type": "text"},
                        "instru_socioe" : {"type": "text"},
                        "tipo_pob_apo_otro" : {"type": "text"},
                        "monto_apoyo_otor" : {"type": "text"},
                        "indic_h" : {"type": "text"},
                        "instru_socioe_cual" : {"type": "text"},
                        "indic_e" : {"type": "text"},
                        "nombre_apoyo" : {"type": "text"},
                        "cuaps_folio" : {"type": "text"},
                        "otro_period_apoyo" : {"type": "text"},
                        "indic_m" : {"type": "text"},
                        "nombre_componente" : {"type": "text"},
                        "actualiza_padron" : {"type": "text"},
                        "indic_i" : {"type": "text"},
                        "period_apoyo_cod" : {"type": "text"},
                        "tipo_apoyo_otro" : {"type": "text"},
                        "indic_a" : {"type": "text"},
                        "tipo_pob_apo_cod" : {"type": "text"},
                        "ap_compo" : {"type": "text"},
                        "tipo_padron" : {"type": "text"},
                        "period_monto_cod" : {"type": "text"},
                        "observaciones" : {"type": "text"},
                        "indic_o" : {"type": "text"},
                        "indic_d" : {"type": "text"},
                        "indic_t" : {"type": "text"},
                        "tipo_apoyo_serv" : {"type": "text"},
                        "tem_apoyo" : {"type": "text"},
                        "indic_b" : {"type": "text"},
                        "periodicidad_padron" : {"type": "text"},
                        "indic_q" : {"type": "text"},
                        "apoyo_gen_padron" : {"type": "text"},
                        "tipo_apoyo_esp" : {"type": "text"},
                        "otro_tipo_padron" : {"type": "text"},
                        "period_monto" : {"type": "text"},
                        "pob_compo" : {"type": "text"},
                        "indic_f" : {"type": "text"},
                        "indic_k" : {"type": "text"},
                        "indic_n" : {"type": "text"},
                        "tipo_pob_apo" : {"type": "text"},
                        "tipo_apoyo_mon" : {"type": "text"},
                        "indic_j" : {"type": "text"},
                        "tipo_apoyo_obra" : {"type": "text"},
                        "id_componente" : {"type": "text"},
                        "otro_tipo_apoyo" : {"type": "text"},
                        "tiene_componentes" : {"type": "text"},
                        "indic_g" : {"type": "text"},
                        "indic_r" : {"type": "text"},
                        "period_apoyo" : {"type": "text"},
                        "id_apoyo" : {"type": "text"},
                        "tipo_apoyo_cap" : {"type": "text"},
                        "indic_c" : {"type": "text"},
                        "tem_apoyo_otra_esp" : {"type": "text"},
                        "descr_apoyo" : {"type": "text"},
                        "tipos" : {"type": "text"},
                        "tipo_apoyo_sub" : {"type": "text"},
                        "indic_l" : {"type": "text"}
            }},
            "padrones" :{
                "properties":{
                    "origen_dep" : {"type": "text"},
                    "sector" : {"type": "text"},
                    "cd_padron" : {"type": "text"},
                    "cd_programa" : {"type": "text"},
                    "cd_dependencia" : {"type": "text"},
                    "anio" : {"type": "text"},
                    "nb_dependencia" : {"type": "text"},
                    "nb_depen_corto": {"type": "text"}
                }}
            }}},
        "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0}
        }

def catalog(event, context):
    """Lambda Function to update pub catalog index at s3 event.
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

                json_string = json_string
                to_elastic_string = json_string.replace("'",'"').encode('utf-8')
                connection = http.client.HTTPConnection(elastic_address)
                connection.request('POST', url=endpoint, headers = headers_post, body=to_elastic_string)
                response = connection.getresponse()
                print("Returned status code: ", response.status)
                print("Returned status text ", response.read())

            print('Iteración: ' + str(count))
            count += 1

        # Save json result
        s3.put_object(Body= to_elastic_string,
                Bucket = "publicaciones-sedesol",
                Key = "catalogo_cuaps_pub.temporal")

    except Exception as e:
        raise e

