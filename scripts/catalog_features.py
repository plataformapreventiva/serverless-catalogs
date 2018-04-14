#!/usr/bin/env/ python3

from boto3 import client
from sqlalchemy import create_engine
import io
import numpy as np
import pandas as pd
import pdb
import psycopg2
import os

s3 = client('s3')
source_bucket = 'pub-raw'
source_key = 'diccionarios/pub-programas/catalogo_pub.csv'
save_bucket = ''
save_key = ''

# Fetch S3 catalog

try:
    response = s3.get_object(Bucket = source_bucket, Key = source_key)
    pub_catalog = pd.read_csv(io.BytesIO(response['Body'].read()), delimiter = '|')
except Exception as e:
    print(e)

# Creating ID
pub_catalog['cd_origen'] = pub_catalog['nb_origen'].str[0].replace(np.nan, '', regex = True)
pub_catalog[['cd_origen', 'cd_programa', 'anio']] = pub_catalog[['cd_origen', 'cd_programa', 'anio']].apply(lambda x: x.astype(str))
pub_catalog['cd_cuaps'] = pub_catalog[['cd_origen', 'cd_programa', 'anio']].apply(lambda x: '-'.join(x), axis = 1)

# Fetch PostgreSQl DB

#parser = argparse.ArgumentParser(description = 'Temporary fix for PG engine creation')
#parser.add_argument('-db_url', type = str, help = 'Database URL')
#args = parser.parse_args()
#db_url = args.db_url

engine = create_engine(os.environ.get('PGURL'))

try:
    cuaps_programas = pd.read_sql_query('select * from raw.cuaps_programas', con = engine)
except Exception as e:
    print(e)

# ID creation

folio =  cuaps_programas['cuaps_folio'].str.split('/')
cuaps_programas['clave_orden_gob'] = [item[1] for item in folio]
cuaps_programas['anio'] = [item[2] for item in folio]
cuaps_programas['cd_cuaps'] = cuaps_programas[['clave_orden_gob', 'chr_clave_prespupuestal_pro', 'anio']].apply(lambda x: '-'.join(x), axis = 1)


# Feature creation

def orden_gobierno(clave):
    if clave == 'F':
        result = 'Federal'
    elif clave == 'E':
        result = 'Estatal'
    elif clave == 'M':
        result = 'Municipal'
    else:
        result = ''
    return(result)

def gather(df, key, value, cols):
    id_vars = [col for col in df.columns if col not in cols]
    id_values = cols
    var_name = key
    value_name = value
    return(pd.melt(df, id_vars, id_values, var_name, value_name))

def check_presence(word, dummy, prefix, recode_dict):
    if dummy == 0:
        result = ''
    else:
        result = word.replace(prefix, '')
        for key, value in recode_dict.items():
            result = result.replace(key, value)
    return(result)


def dummy_to_list(df, prefix, id_col, recode_dict, new_name, value = 'presente'):
    """

    """
    column_list = [col for col in df if str(col).startswith(prefix) or str(col) == id_col]
    selected_df = df[column_list]
    dummy_list = [col for col in column_list if col != id_col]
    long_df = gather(df = selected_df, key = prefix, value = value, cols = dummy_list)
    long_df[value] = long_df.apply(lambda x: check_presence(x[prefix], x[value], prefix, recode_dict), axis = 1)
    pivot_df = long_df.pivot(index = id_col, columns = prefix, values = value)
    pivot_df[new_name] = pivot_df[dummy_list].apply(lambda x: '-'.join(x), axis = 1)
    pivot_df = pivot_df.drop(columns = dummy_list)
    df = df.join(pivot_df, on = id_col)
    return(df)

derechos_dict = {'alim': 'Alimentación', 'edu':'Educación', 'beco':'Bienestar Económico', 'mam':'Medio Ambiente', 'nodis':'No Discriminación', 'sal':'Salud', 'segsoc':'Seguridad Social', 'tra':'Trabajo', 'viv':'Vivienda', 'ning':'Ninguno'}

apoyos_dict = {'mon':'Monetario', 'sub':'Subsidio', 'esp':'Especie', 'obra':'Obra', 'serv':'Servicio', 'cap':'Capacitación', 'otro':'Otro'}

cuaps_programas['orden_gob'] = cuaps_programas['clave_orden_gob'].apply(lambda x: orden_gobierno(x))
cuaps_programas = dummy_to_list(cuaps_programas, 'der_social_', 'cd_cuaps', derechos_dict, 'derechos_sociales')
cuaps_programas = cuaps_programas[['cuaps_folio', 'cd_cuaps', 'orden_gob', 'derechos_sociales']]

### Tipo de apoyo data

try:
    cuaps_apoyos = pd.read_sql_query('select * from raw.cuaps_componentes', con = engine)
except Exception as e:
    print(e)

selected_vars = [col for col in cuaps_apoyos if str(col).startswith('tipo_apoyo_') or str(col) == 'cuaps_folio']
cuaps_apoyos = cuaps_apoyos[selected_vars]
apoyos_list = [x for x in selected_vars if x != 'cuaps_folio']
cuaps_apoyos = cuaps_apoyos.groupby('cuaps_folio')[apoyos_list].agg('sum')
cuaps_apoyos = cuaps_apoyos.reset_index(level = ['cuaps_folio'])
cuaps_apoyos = dummy_to_list(cuaps_apoyos, 'tipo_apoyo_', 'cuaps_folio', apoyos_dict, 'tipos_apoyos')

# Temporary fix for trimming

cuaps_programas = cuaps_programas.apply(lambda x: x.str.strip())
cuaps_apoyos = cuaps_apoyos[['cuaps_folio', 'tipos_apoyos']]
cuaps_apoyos = cuaps_apoyos.apply(lambda x: x.str.strip())

cuaps_features = cuaps_programas.merge(cuaps_apoyos, on = 'cuaps_folio', how = 'inner')
catalog_features = pub_catalog.merge(cuaps_features, on = 'cd_cuaps', how = 'inner')

catalog_features.to_csv('catalogo_cuaps_features.csv', sep = '|', index = False)

s3 = boto3.resource('s3')
bucket = 'publicaciones-sedesol'

s3.Bucket(bucket).upload_file('catalogo_cuaps_features.csv', 'catalogo_cuaps_features.csv')
