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
            "nb_programa" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
            "DGAE" : {"type": "text"},
            "DGAIP" : {"type": "text"},
            "anio" : {"type": "text"},
            "obj_gral_prog" : {"type" : "text"},
            "cuaps_folio" : {"type": "text"},
            "derechos_sociales" : { "type":"text", 
                                    "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
            "nb_origen" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
            "nb_subp1" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
            "origen" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
            "apoyos" :{
                    "properties":{
                        "indic_s" : {"type": "keyword"},
                        "cuenta_info_geo" : {"type": "keyword"},
                        "obj_compo" : {"type": "keyword"},
                        "indic_p" : {"type": "keyword"},
                        "otro_actualiza_padron" : {"type": "keyword"},
                        "instru_socioe" : {"type": "keyword"},
                        "tipo_pob_apo_otro" : {"type": "keyword"},
                        "monto_apoyo_otor" : {"type": "keyword"},
                        "indic_h" : {"type": "keyword"},
                        "instru_socioe_cual" : {"type": "keyword"},
                        "indic_e" : {"type": "keyword"},
                        "nombre_apoyo" : {"type": "keyword"},
                        "cuaps_folio" : {"type": "keyword"},
                        "otro_period_apoyo" : {"type": "keyword"},
                        "indic_m" : {"type": "keyword"},
                        "nombre_componente" : {"type": "keyword"},
                        "actualiza_padron" : {"type": "keyword"},
                        "indic_i" : {"type": "keyword"},
                        "period_apoyo_cod" : {"type": "keyword"},
                        "tipo_apoyo_otro" : {"type": "keyword"},
                        "indic_a" : {"type": "keyword"},
                        "tipo_pob_apo_cod" : {"type": "keyword"},
                        "ap_compo" : {"type": "keyword"},
                        "tipo_padron" : {"type": "keyword"},
                        "period_monto_cod" : {"type": "keyword"},
                        "observaciones" : {"type": "keyword"},
                        "indic_o" : {"type": "keyword"},
                        "indic_d" : {"type": "keyword"},
                        "indic_t" : {"type": "keyword"},
                        "tipo_apoyo_serv" : {"type": "keyword"},
                        "tem_apoyo" : {"type": "keyword"},
                        "indic_b" : {"type": "keyword"},
                        "periodicidad_padron" : {"type": "keyword"},
                        "indic_q" : {"type": "keyword"},
                        "apoyo_gen_padron" : {"type": "keyword"},
                        "tipo_apoyo_esp" : {"type": "keyword"},
                        "otro_tipo_padron" : {"type": "keyword"},
                        "period_monto" : {"type": "keyword"},
                        "pob_compo" : {"type": "keyword"},
                        "indic_f" : {"type": "keyword"},
                        "indic_k" : {"type": "keyword"},
                        "indic_n" : {"type": "keyword"},
                        "tipo_pob_apo" : {"type": "keyword"},
                        "tipo_apoyo_mon" : {"type": "keyword"},
                        "indic_j" : {"type": "keyword"},
                        "tipo_apoyo_obra" : {"type": "keyword"},
                        "id_componente" : {"type": "keyword"},
                        "otro_tipo_apoyo" : {"type": "keyword"},
                        "tiene_componentes" : {"type": "keyword"},
                        "indic_g" : {"type": "keyword"},
                        "indic_r" : {"type": "keyword"},
                        "period_apoyo" : {"type": "keyword"},
                        "id_apoyo" : {"type": "keyword"},
                        "tipo_apoyo_cap" : {"type": "keyword"},
                        "indic_c" : {"type": "keyword"},
                        "tem_apoyo_otra_esp" : {"type": "keyword"},
                        "descr_apoyo" : {"type": "keyword"},
                        "tipos" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
                        "tipo_apoyo_sub" : {"type": "keyword"},
                        "indic_l" : {"type": "keyword"}
            }},
            "padrones" :{
                "properties":{
                    "origen_dep" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
                    "sector" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
                    "cd_padron" : { "type":"text"},
                    "cd_programa" : { "type":"text"},
                    "cd_dependencia" : { "type":"text"},
                    "anio" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
                    "nb_dependencia" : { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } },
                    "nb_depen_corto": { "type":"text", 
                                     "fields": { "keyword": { "type": "keyword", 
                                                              "ignore_above": 256 } } }
                }},
            "criterios_focalizacion": {"type": "text",
                                       "fields": {"keyword": {"type": "keyword",
                                                              "ignore_above": 256} } },
            "tiene_perfil": {"type": "integer"},
            "cve_ent": {"type": "text"},
            "nom_ent": {"type": "text",
                                       "fields": {"keyword": {"type": "keyword",
                                                              "ignore_above": 256} } }
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

