#!/usr/bin/env python
import ast
import io
import numpy as np
import os
import pandas as pd
import psycopg2
import re
import unittest

from dotenv import load_dotenv, find_dotenv
from boto3 import client, resource
from sqlalchemy import create_engine

# Load env variables
load_dotenv(find_dotenv())
save_bucket = ''
save_key = ''

# Clients
s3 = client('s3')
s3_ = resource('s3')
engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(os.getenv('POSTGRES_USER'), os.getenv('POSTGRES_PASSWORD'),os.getenv('PGHOST'), os.getenv('PGPORT'), os.getenv('PGDATABASE')))

#################################
## Functions
#################################

def gather(df, key, value, cols):
    id_vars = [col for col in df.columns if col not in cols]
    id_values = cols
    var_name = key
    value_name = value
    return(pd.melt(df, id_vars, id_values, var_name, value_name))

xstr = lambda s: s or ""

def combine_cols(x, cond_1, value_1, cond_2, value_2):
    if x == cond_1:
        value = xstr(value_1)
    elif x == cond_2:
        value = xstr(value_2)
    else:
        value = ""
    return(value)

def check_presence(word, dummy, prefix, recode_dict):
    if dummy == 0:
        result = ""
    else:
        result = word.replace(prefix, "")
        for key, value in recode_dict.items():
            result = result.replace(key, value)
    return(result)

def surround_list(x):
    result = '["' + x + '"]'
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
    pivot_df[new_name] = pivot_df[dummy_list].apply(lambda x: '","'.join(x), axis = 1)
    pivot_df[new_name] = pivot_df[new_name].apply(surround_list)
    pivot_df = pivot_df.drop(dummy_list, axis=1)
    df = df.join(pivot_df, on = id_col)
    return(df)

def create_list(col):
    result = col.apply(lambda x: '["' + '","'.join(x) + '"]')
    return(result)

def flatten_df(df, duplicated_subset, columns_to_flatten):
    flattened_df = df.groupby(duplicated_subset)[columns_to_flatten].apply(create_list)
    flattened_df.reset_index(level=duplicated_subset, inplace=True)
    return(flattened_df)

def parse_list(x):
    if pd.isnull(x):
        string_list = '[]'
    else:
        string_list = x
    parsed_list = ast.literal_eval(string_list)
    clean_list = [x for x in parsed_list if x != ""]
    return(clean_list)

################################
## Recode dictionaries
################################

derechos_dict = {'alim': 'Alimentación', 'edu':'Educación', 'beco':'Bienestar Económico', 'mam':'Medio Ambiente', 'nodis':'No Discriminación', 'sal':'Salud', 'segsoc':'Seguridad Social', 'tra':'Trabajo', 'viv':'Vivienda', 'ning':'Ninguno'}

apoyos_dict = {'mon':'Monetario', 'sub':'Subsidio', 'esp':'Especie', 'obra':'Obra', 'serv':'Servicio', 'cap':'Capacitación', 'otro':'Otro'}


#################################
## Load PUB_CUAPS joining Catalog
#################################

try:
    source_bucket = 'publicaciones-sedesol'
    source_key = 'cuaps/Programas_DGAIP_DGAE.xlsx'
    response = s3.get_object(Bucket = source_bucket, Key = source_key)
    joining_catalog = pd.read_excel(io.BytesIO(response['Body'].read()),
                                    converters={'DGAE':str,'DGAIP':str})
    joining_catalog = joining_catalog[["DGAIP","DGAE"]].dropna()
    joining_catalog = joining_catalog.drop_duplicates()
except Exception as e:
    print(e)

#################################
## CUAPS
#################################
try:
    cuaps_programas = pd.read_sql_query('select * from clean.cuaps_programas', con = engine)
except Exception as e:
    print(e)


# Creating ID
cuaps_programas['nb_orden_gob'] = cuaps_programas['orden_gob'].replace({1:'FEDERAL', 2:'ESTATAL', 3:'MUNICIPAL'})

# Paste all dummy variables for ES to look for
cuaps_programas = dummy_to_list(cuaps_programas, 'der_social_', 'cuaps_folio', derechos_dict, 'derechos_sociales')

# Create clean ID for CUAPS data
cuaps_programas['DGAE'] = cuaps_programas.apply(lambda row:
        combine_cols(x=row['orden_gob'],
            cond_1=1, value_1=row['chr_clave_prespupuestal_pro'],
            cond_2=2, value_2=row['cuaps_folio']), axis = 1)


# Temporary fix for IDs
cuaps_programas['DGAE'] = np.where(cuaps_programas.DGAE.str.contains('21111'), cuaps_programas['cuaps_folio'], cuaps_programas['DGAE'])
cuaps_programas['DGAE'] = cuaps_programas['DGAE'].replace(regex={r'AGUASCALIENTES':r'AGS', r'JALISCO':r'JAL', r'OAXACA':r'OAX', r'TABASCO':r'TAB'}).values

# Select key variables and merge keys
cuaps_programas_selected = cuaps_programas[['DGAE', 'cuaps_folio',
    'chr_clave_prespupuestal_pro', 'derechos_sociales']]
cuaps_programas_selected = cuaps_programas_selected.apply(lambda x: x.str.strip())
cuaps_programas_selected['DGAE'] = cuaps_programas_selected.DGAE.replace("", cuaps_programas_selected.cuaps_folio)


cuaps_programas_ids = cuaps_programas_selected.set_index('DGAE').join(joining_catalog.set_index('DGAE'), how='left')
cuaps_programas_ids = cuaps_programas_ids.reset_index()
cuaps_programas_ids['id_pub'] = np.where(pd.notnull(cuaps_programas_ids['DGAIP']), cuaps_programas_ids['DGAIP'], cuaps_programas_ids['DGAE'])

cuaps_programas_ids['derechos_sociales'] = cuaps_programas_ids.derechos_sociales.apply(parse_list)

cuaps_programas_ids.fillna("null", inplace=True)
cuaps_programas_ids.replace(r'"+', '', regex=True, inplace=True)

### Tipo de apoyo data
# TO DO: feature construction de variables de apoyos
try:
    cuaps_apoyos = pd.read_sql_query('select * from raw.cuaps_componentes', con = engine)
except Exception as e:
    print(e)

cuaps_apoyos.drop(['csc_estatus_cuaps_fk', 'chr_nombre_programa_cuaps', 'actualizacion_sedesol', 'data_date'], inplace=True, axis=1)
strings_dict = {colname : "null" for colname in cuaps_apoyos.columns.values if cuaps_apoyos[colname].dtype == 'object'}
numeric_dict = {colname : 0 for colname in cuaps_apoyos.columns.values if
        cuaps_apoyos[colname].dtype != 'object'}
apoyos_recode_dict = {**strings_dict, **numeric_dict}
cuaps_apoyos.fillna(value=apoyos_recode_dict, inplace=True)

float_cols = [colname for colname in cuaps_apoyos.columns.values if
        cuaps_apoyos[colname].dtype == 'float64']
cuaps_apoyos[float_cols] = cuaps_apoyos[float_cols].astype(int)


# List features
list_cols = [col for col in cuaps_apoyos if str(col).startswith('tipo_apoyo_') or col == 'cuaps_folio']
cuaps_apoyos_flattened = cuaps_apoyos.groupby('cuaps_folio')[list_cols[1:8]].agg('sum')
cuaps_apoyos_flattened.reset_index(level = ['cuaps_folio'], inplace=True)
cuaps_apoyos_reduced = dummy_to_list(cuaps_apoyos_flattened[list_cols], 'tipo_apoyo_', 'cuaps_folio', apoyos_dict, 'tipos_apoyos')[['cuaps_folio', 'tipos_apoyos']]

cuaps_features = cuaps_programas_ids.merge(cuaps_apoyos_reduced, left_on='cuaps_folio', right_on='cuaps_folio', how='left')
cuaps_features['tipos_apoyos'] = cuaps_features.tipos_apoyos.apply(parse_list)


#################################
## PUB catalog
#################################

try:
    source_bucket = 'pub-raw'
    source_key = 'diccionarios/catalogo_programas.csv'
    response = s3.get_object(Bucket = source_bucket, Key = source_key)
    pub_catalog = pd.read_csv(io.BytesIO(response['Body'].read()), delimiter = '|')
except Exception as e:
    print(e)

pub_catalog = pub_catalog.drop(pub_catalog.columns[0], axis = 1)
pub_catalog['anio'] = pub_catalog.anio.astype(float).astype(int).astype(str)
pub_catalog['cd_dependencia'] = pub_catalog.cd_dependencia.astype(float).astype(int).astype(str)
pub_catalog.fillna("null", inplace = True)
unique_cols = ['cd_programa', 'anio', 'nb_origen', 'nb_programa', 'origen']
pub_catalog_unique = pub_catalog[unique_cols].drop_duplicates()
pub_catalog_subp = flatten_df(pub_catalog, duplicated_subset=['cd_programa', 'anio'], columns_to_flatten=['nb_subp1'])

pub_catalog_core = pub_catalog_subp.merge(pub_catalog_unique, how = 'left')
pub_catalog_core.replace('\\xa0', ' ', regex=True, inplace=True)

# List features
pub_catalog = flatten_df(pub_catalog, duplicated_subset=['cd_programa',
    'anio'], columns_to_flatten=['cd_dependencia', 'cd_padron', 'iduni',
        'origen_dep', 'nb_dependencia', 'nb_depen_corto', 'sector'])

pub_features = pub_catalog.merge(pub_catalog_core, how = 'left')
pub_features.drop_duplicates(subset = ['anio', 'cd_programa'], inplace = True)

pub_current = pub_features[pub_features.anio.astype(int) > 2016]
pub_history = pub_features[pub_features.anio.astype(int) <= 2016]

pub_current_features = pub_current.merge(cuaps_features, left_on='cd_programa',
        right_on='id_pub', how='outer')
catalog_features = pd.concat([pub_current_features, pub_history])

selected_vars = ['cd_programa', 'id_pub', 'nb_programa', 'iduni', 'DGAE', 'DGAIP', 'cd_dependencia', 'nb_dependencia', 'nb_depen_corto', 'anio', 'origen', 'nb_origen', 'origen_dep', 'cd_padron', 'cuaps_folio', 'nb_subp1', 'derechos_sociales', 'tipos_apoyos']

listed_vars = ['iduni', 'cd_dependencia', 'nb_dependencia', 'nb_depen_corto', 'origen_dep', 'cd_padron', 'nb_subp1']

for col in listed_vars:
    catalog_features[col] = catalog_features[col].apply(parse_list)

catalog_features = catalog_features[selected_vars]
catalog_features.fillna("null", inplace=True)
header_test = unittest.TestCase()
header_test.assertTrue(catalog_features.columns.values.tolist() == selected_vars)

#################################
# Save files
#################################

catalog_features.to_csv('catalogo_cuaps_features_list.csv', sep = '|', index = False, encoding = 'utf-8')

#Todo: reemplazar '' por "" desde Python
bucket = 'publicaciones-sedesol'
s3_.Bucket(bucket).upload_file('catalogo_cuaps_features_list.csv', 'catalogo_cuaps_features_list.csv')


