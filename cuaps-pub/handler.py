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
#elastic_address = "search-pp-search-pjomldcgauebmkfa26wkjfiv4y.us-west-2.es.amazonaws.com"
elastic_address = os.getenv('HOST_ES')
elastic_index = "cuaps-pub_json"
elastic_type = "catalogo"


apoyos_None = {
        "cuaps_folio": None,
        "tiene_componentes": None,
        "id_componente": None,
        "nombre_componente": None,
        "obj_compo": None,
        "pob_compo": None,
        "ap_compo": None,
        "id_apoyo": None,
        "nombre_apoyo": None,
        "descr_apoyo": None,
        "tipo_pob_apo_cod": None,
        "tipo_pob_apo": None,
        "tipo_pob_apo_otro": None,
        "tipo_apoyo_mon": None,
        "tipo_apoyo_sub": None,
        "tipo_apoyo_esp": None,
        "tipo_apoyo_obra": None,
        "tipo_apoyo_serv": None,
        "tipo_apoyo_cap": None,
        "tipo_apoyo_otro": None,
        "otro_tipo_apoyo": None,
        "period_apoyo_cod": None,
        "period_apoyo": None,
        "otro_period_apoyo": None,
        "monto_apoyo_otor": None,
        "period_monto_cod": None,
        "period_monto": None,
        "indic_a": None,
        "indic_b": None,
        "indic_c": None,
        "indic_d": None,
        "indic_e": None,
        "indic_f": None,
        "indic_g": None,
        "indic_h": None,
        "indic_i": None,
        "indic_j": None,
        "indic_k": None,
        "indic_l": None,
        "indic_m": None,
        "indic_n": None,
        "indic_o": None,
        "indic_p": None,
        "indic_q": None,
        "indic_r": None,
        "indic_s": None,
        "indic_t": None,
        "tem_apoyo": None,
        "tem_apoyo_otra_esp": None,
        "apoyo_gen_padron": None,
        "tipo_padron": None,
        "otro_tipo_padron": None,
        "periodicidad_padron": None,
        "actualiza_padron": None,
        "otro_actualiza_padron": None,
        "cuenta_info_geo": None,
        "instru_socioe": None,
        "instru_socioe_cual": None,
        "observaciones": None,
        "tipos": None}

indexDoc = {
    "dataRecord": {
        "properties": {
            "cd_programa" : {"type": "string"},
            "id_pub" : {"type": "string"},
            "nb_programa" : {"type": "string"},
            "iduni" : {"type": "string"},
            "DGAE" : {"type": "string"},
            "DGAIP" : {"type": "string"},
            "cd_dependencia" : {"type": "string"},
            "nb_dependencia" : {"type": "string"},
            "nb_depen_corto" : {"type": "string"},
            "anio" : {"type": "string"},
            "origen" : {"type": "string"},
            "nb_origen" : {"type": "string"},
            "origen_dep" : {"type": "string"},
            "cd_padron" : {"type": "string"},
            "cuaps_folio" : {"type": "string"},
            "nb_subp1" : {"type": "string"},
            "derechos_sociales" : {"type": "string"},
            "tipos_apoyos" : {"type": "string"}
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
    }

indexDoc = {
    "mappings": {
     "catalogo": {
        "properties": {
            "cd_programa" : {"type": "string"},
            "id_pub" : {"type": "string"},
            "nb_programa" : {"type": "string"},
            "iduni" : {"type": "string"},
            "DGAE" : {"type": "string"},
            "DGAIP" : {"type": "string"},
            "cd_dependencia" : {"type": "string"},
            "nb_dependencia" : {"type": "string"},
            "nb_depen_corto" : {"type": "string"},
            "anio" : {"type": "string"},
            "origen" : {"type": "string"},
            "nb_origen" : {"type": "string"},
            "origen_dep" : {"type": "string"},
            "cd_padron" : {"type": "string"},
            "cuaps_folio" : {"type": "string"},
            "nb_subp1" : {"type": "string"},
            "derechos_sociales" : {"type": "string"},
            "apoyos" : {"type": "object"},
            "padrones" : {"type": "object"}
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    }
    }


def catalog(event, context):
    """Lambda Function to update pub catalog index at s3 event.
    """

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    try:
        # define arguments
        por_linea = True
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

        #if lista[-1] == b'':
        #    lista = lista[:-1]

        # Data sctructure
        keys =list(indexDoc["mappings"]["catalogo"]['properties'].keys())
        values = [ "%"+str(x)+"%" for x in keys]
        json_struct = str(dict(zip(keys, values))).replace("%',", "%',\n")

        for row in lista:
            row = row.decode('utf-8')
            row = row.replace("b'","").replace("'","").replace('""','"').replace('}"', '}')
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
                            elif indexDoc["mappings"]["catalogo"]["properties"][headers[pos]]["type"] == 'object':
                                _data = _data.replace('"%' + header + '%"',
                                        str(yaml.load(row[pos].replace("'",'"'))))
                            else:
                                _data = _data.replace('%' + header + '%', row[pos])
                        except:

                            try:
                                _data = _data.replace('"%' + header + '%"',
                                    str(json.loads(row[pos])))
                            except:
                                pass
                    print(pos)
                    pos += 1
                if id_column is not None:
                    index_row = {"index": {"_index": elastic_index,
                                           "_type": elastic_type,
                                           '_id': row[headers_position[id_column]]}}
                else:
                    index_row = {"index": {"_index": elastic_index, "_type": elastic_type}}
                json_string = json.dumps(index_row) + "\n" + _data + "\n"
                # to_elastic_string += json_string
                if por_linea == True:
                    to_elastic_string = json_string.replace("'",'"').encode('utf-8')
                    connection = http.client.HTTPConnection(elastic_address)
                    connection.request('POST', url=endpoint, headers = headers_post, body=to_elastic_string)
                    response = connection.getresponse()

                    print("Returned status code: ", response.status)
                    print("Returned status text ", response.read())


            count += 1

        to_elastic_string = to_elastic_string.replace("'",'"').encode('utf-8')
        connection = http.client.HTTPConnection(elastic_address)

        # Remove old index
        response = requests.delete("http://" + elastic_address + "/" + elastic_index)
        print("Returned from delete request: ", response)
        # Create index
        response = requests.put("http://" + elastic_address + "/" + elastic_index, data=json.dumps(indexDoc))
        print("Returned from delete request: ", response)

        # Update ElasticSearch index
        connection = http.client.HTTPConnection(elastic_address)
        connection.request('POST', url=endpoint, headers = headers_post, body=to_elastic_string)
        response = connection.getresponse()

        print("Returned status code: ", response.status)
        print("Returned status text ", response.read())

        # Save json result
        s3.put_object(Body= to_elastic_string,
                Bucket = "publicaciones-sedesol",
                Key = "catalogo_cuaps_pub.temporal")

    except Exception as e:

        raise e

