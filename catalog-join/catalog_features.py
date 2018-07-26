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
engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.\
        format(os.getenv('POSTGRES_USER'),
            os.getenv('POSTGRES_PASSWORD'),
            os.getenv('PGHOST'),
            os.getenv('PGPORT'),
            os.getenv('PGDATABASE')))

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

def parse_list(x):
    if pd.isnull(x):
        string_list = '[]'
    else:
        string_list = x
    parsed_list = ast.literal_eval(string_list)
    clean_list = [x for x in parsed_list if x != ""]
    return(clean_list)

def dummy_to_list(df, prefix, id_col, recode_dict, new_name, value = 'presente'):
    """

    """
    column_list = [col for col in df if str(col).startswith(prefix) or str(col) == id_col]
    selected_df = df[column_list]
    dummy_list = [col for col in column_list if col != id_col]
    long_df = gather(df = selected_df, key = prefix, value = value, cols = dummy_list)
    long_df[value] = long_df.apply(lambda x: check_presence(x[prefix], x[value], prefix, recode_dict), axis = 1)
    pivot_df = long_df.pivot(index = id_col, columns = prefix, values = value)
    # podríamos usar flatten_df a partir de acá
    pivot_df[new_name] = pivot_df[dummy_list].apply(lambda x: '","'.join(x), axis = 1)
    pivot_df[new_name] = pivot_df[new_name].apply(surround_list).apply(parse_list)
    pivot_df = pivot_df.drop(dummy_list, axis=1)
    df = df.join(pivot_df, on = id_col)
    return(df)

derechos_dict = {'alim': 'Alimentación', 'edu':'Educación', 'beco':'Bienestar Económico', 'mam':'Medio Ambiente', 'nodis':'No Discriminación', 'sal':'Salud', 'segsoc':'Seguridad Social', 'tra':'Trabajo', 'viv':'Vivienda', 'ning':'Ninguno'}

apoyos_dict = {'mon':'Monetario', 'sub':'Subsidio', 'esp':'Especie', 'obra':'Obra', 'serv':'Servicio', 'cap':'Capacitación', 'otro':'Otro'}

def create_list(col):
    result = col.apply(lambda x: '["' + '","'.join(x) + '"]')
    return(result)

def flatten_df(df, duplicated_subset, columns_to_flatten):
    flattened_df = df.groupby(duplicated_subset)[columns_to_flatten].apply(create_list)
    flattened_df.reset_index(level=duplicated_subset, inplace=True)
    for col in columns_to_flatten:
        flattened_df[col] = flattened_df[col].apply(parse_list)
    return(flattened_df)


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
## CUAPS: Programas
#################################

try:
    cuaps_programas = pd.read_sql_query('select * from clean.cuaps_programas', con = engine)
except Exception as e:
    print(e)

# Clean data
cuaps_programas['chr_nombre_programa_cuaps'] = cuaps_programas['chr_nombre_programa_cuaps'].str.strip()
cuaps_programas['nom_entidad_federativa'] = cuaps_programas['nom_entidad_federativa'].str.strip()
cuaps_programas['cve_entidad_federativa'] = cuaps_programas.\
                                            cve_entidad_federativa.\
                                            fillna(0).\
                                            astype(int).\
                                            astype(str).\
                                            str.pad(2, fillchar='0').\
                                            replace({'00':'null'})

# Create clean ID for CUAPS data
cuaps_programas['DGAE'] = cuaps_programas.apply(lambda row:
        combine_cols(x=row['orden_gob'],
            cond_1=1, value_1=row['chr_clave_prespupuestal_pro'],
            cond_2=2, value_2=row['cuaps_folio']), axis = 1)
cuaps_programas['DGAE'] = np.where(cuaps_programas.DGAE.str.contains('21111'), cuaps_programas['cuaps_folio'], cuaps_programas['DGAE'])
cuaps_programas['DGAE'] = cuaps_programas.DGAE.replace("", cuaps_programas.cuaps_folio)
cuaps_programas['DGAE'] = cuaps_programas['DGAE'].replace(regex={r'AGUASCALIENTES':r'AGS', r'JALISCO':r'JAL', r'OAXACA':r'OAX', r'TABASCO':r'TAB'}).values

cuaps_programas_ids = cuaps_programas.set_index('DGAE').join(joining_catalog.set_index('DGAE'), how='left').reset_index()
cuaps_programas_ids['id_pub'] = np.where(pd.notnull(cuaps_programas_ids['DGAIP']), cuaps_programas_ids['DGAIP'], cuaps_programas_ids['DGAE'])

cuaps_programas_ids.fillna("null", inplace=True)
cuaps_programas_ids.replace(r'"+', '', regex=True, inplace=True)

# Create derechos feature
cuaps_programas_features = dummy_to_list(cuaps_programas_ids, 'der_social_',
        'cuaps_folio', derechos_dict, 'derechos_sociales')[['id_pub',
        'cuaps_folio', 'DGAE', 'DGAIP', 'derechos_sociales', 'obj_gral_prog',
        'chr_nombre_programa_cuaps', 'nom_entidad_federativa', 'cve_entidad_federativa']].\
                rename(columns={'nom_entidad_federativa':'nom_ent',
                    'cve_entidad_federativa':'cve_ent'})


#################################
## CUAPS: Focalización
#################################

try:
    cuaps_criterios = pd.read_sql_query('select * from raw.cuaps_criterios', con = engine)
except Exception as e:
    print(e)

strings_dict = {colname : "null" for colname in cuaps_criterios.columns.values if cuaps_criterios[colname].dtype == 'object'}
numeric_dict = {colname : 0 for colname in cuaps_criterios.columns.values if cuaps_criterios[colname].dtype != 'object'}
criterios_recode_dict = {**strings_dict, **numeric_dict}
cuaps_criterios = cuaps_criterios.fillna(value=criterios_recode_dict) \
        .replace(r'"+', '', regex=True)
float_cols = [colname for colname in cuaps_criterios.columns.values if cuaps_criterios[colname].dtype == 'float64']
cuaps_criterios[float_cols] = cuaps_criterios[float_cols].astype(int)
criterios_dic = {'adultos-mayores':[36, 1754, 1755, 1756, 1757],
        'migrantes':[37], 'embarazadas':[38], 'jefas-de-familia':[39],
        'padres-o-madres-solos':[40], 'enfermos-cronicos':[41],
        'personas-con-discapacidad':[43, 170, 171, 172, 173, 174, 175, 176],
        'poblacion-indigena':[42, 167, 168, 169],
        'otro-grupo-vulnerable':[44],
        'pobreza':[31, 32, 33],
        'vulnerabilidad':[34, 35],
        'marginacion':[134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
            145]}

cuaps_criterios['criterios_focalizacion'] = cuaps_criterios.csc_configuracion_foc.astype(str)
for key in criterios_dic.keys():
    value_list = criterios_dic[key]
    for x in value_list:
        cuaps_criterios['criterios_focalizacion'] = cuaps_criterios.criterios_focalizacion.replace(str(x), key).replace('-', ' ', regex=True)

cuaps_criterios.criterios_focalizacion.replace(r'\d+', '', regex=True, inplace=True)

# Clean data, remove missing values
cuaps_criterios_features = flatten_df(cuaps_criterios,
        duplicated_subset=['cuaps_folio'], columns_to_flatten=['criterios_focalizacion'])


#################################
## CUAPS: Apoyos
#################################
# TO DO: feature construction de variables de apoyos
# TO DO: write function for cleaning data

try:
    cuaps_apoyos = pd.read_sql_query('select * from raw.cuaps_componentes', con = engine)
except Exception as e:
    print(e)

# Clean data, remove missing values
cuaps_apoyos.drop(['csc_estatus_cuaps_fk', 'chr_nombre_programa_cuaps', 'actualizacion_sedesol', 'data_date'], inplace=True, axis=1)
strings_dict = {colname : "null" for colname in cuaps_apoyos.columns.values if cuaps_apoyos[colname].dtype == 'object'}
numeric_dict = {colname : 0 for colname in cuaps_apoyos.columns.values if cuaps_apoyos[colname].dtype != 'object'}
apoyos_recode_dict = {**strings_dict, **numeric_dict}
cuaps_apoyos = cuaps_apoyos.fillna(value=apoyos_recode_dict) \
        .replace(r'"+', '', regex=True)
float_cols = [colname for colname in cuaps_apoyos.columns.values if cuaps_apoyos[colname].dtype == 'float64']
cuaps_apoyos[float_cols] = cuaps_apoyos[float_cols].astype(int)
cuaps_apoyos['id_apoyo_index'] = cuaps_apoyos.cuaps_folio + '-' + cuaps_apoyos.id_componente.astype(str) + '-' + cuaps_apoyos.id_apoyo.astype(str)

# Dict features
list_cols = [col for col in cuaps_apoyos if str(col).startswith('tipo_apoyo_') or col == 'id_apoyo_index']
tipos_apoyos = dummy_to_list(cuaps_apoyos[list_cols], 'tipo_apoyo_', 'id_apoyo_index', apoyos_dict, 'tipos')[['id_apoyo_index', 'tipos']]
cuaps_apoyos_features = cuaps_apoyos.merge(tipos_apoyos, left_on='id_apoyo_index', right_on='id_apoyo_index', how='left')

cuaps_features = cuaps_programas_features.merge(cuaps_criterios_features, left_on='cuaps_folio', right_on='cuaps_folio')
none_dic_apoyos = [{col:("null" if col!='tipos' else '[]') for col in cuaps_apoyos_features.columns.values}]
cuaps_features['apoyos'] = str(none_dic_apoyos)
for i in cuaps_features.index:
    program_id = cuaps_features.cuaps_folio[i]
    apoyos_data = cuaps_apoyos_features[cuaps_apoyos_features.cuaps_folio == program_id]
    cuaps_features['apoyos'][i] = apoyos_data.to_dict(orient='records')

#################################
## PUB catalog
#################################

try:
    source_bucket = 'pub-raw'
    source_key = 'diccionarios/catalogo_programas.csv'
    response = s3.get_object(Bucket = source_bucket, Key = source_key)
    pub_programas = pd.read_csv(io.BytesIO(response['Body'].read()), delimiter = '|')
except Exception as e:
    print(e)

# Tiramos los programas viejos para quedarnos con valores únicos de clave de
# programa
pub_programas = pub_programas.sort_values(by='anio', ascending=False).drop_duplicates(subset='cd_programa')

# Clean the data a little bit
pub_programas = pub_programas.drop(pub_programas.columns[0], axis=1). \
        fillna("null"). \
        replace({'\\xa0':' ', r'"+':''}, regex=True)

pub_programas['anio'] = pub_programas.anio.astype(float).astype(int).astype(str)
pub_programas['cd_dependencia'] = pub_programas.cd_dependencia.astype(float).astype(int).astype(str)

# Separate unique from listed and dict features
unique_cols = ['cd_programa', 'anio', 'nb_origen', 'nb_programa', 'origen']
pub_programas_unique = pub_programas[unique_cols].drop_duplicates()
pub_programas_subp = flatten_df(pub_programas, duplicated_subset=['cd_programa', 'anio'], columns_to_flatten=['nb_subp1'])
pub_programas_core = pub_programas_subp.merge(pub_programas_unique, how = 'left')

# Create dict features
pub_dict_cols = ['cd_programa', 'anio', 'iduni', 'cd_dependencia', 'cd_padron', 'origen_dep', 'nb_dependencia', 'nb_depen_corto', 'sector']
pub_dict_features = pub_programas[pub_dict_cols]

pub_features = pub_programas_core.copy()
none_dic_padrones = [{col:"null" for col in pub_dict_features.columns.values}]
pub_features['padrones'] = str(none_dic_padrones)

for i in pub_features.index:
    program_id = pub_features.cd_programa[i]
    program_year = pub_features.anio[i]
    padrones_data = pub_dict_features[(pub_dict_features.cd_programa == program_id) & (pub_dict_features.anio == program_year)]
    pub_features['padrones'][i] = padrones_data.to_dict(orient='records')

#pub_features.drop_duplicates(subset=['anio', 'cd_programa'], inplace=True)

catalog_features = pub_features.merge(cuaps_features, left_on='cd_programa', right_on='id_pub', how='outer')

# Fix missing values
catalog_features['cd_programa'] = np.where(pd.isnull(catalog_features.cd_programa), catalog_features.id_pub, catalog_features.cd_programa)
catalog_features['padrones'] = np.where(pd.isnull(catalog_features.padrones), none_dic_padrones, catalog_features.padrones)
catalog_features['derechos_sociales'] = np.where(pd.isnull(catalog_features.derechos_sociales), '[]', catalog_features.derechos_sociales)
catalog_features['obj_gral_prog'] = np.where(pd.isnull(catalog_features.obj_gral_prog), '', catalog_features.obj_gral_prog)
catalog_features['nb_programa']=np.where(pd.isnull(catalog_features.nb_programa), catalog_features.chr_nombre_programa_cuaps, catalog_features.nb_programa)
catalog_features['criterios_focalizacion'] = np.where(pd.isnull(catalog_features.criterios_focalizacion), '[]', catalog_features.criterios_focalizacion)
catalog_features['apoyos'] = np.where(pd.isnull(catalog_features.apoyos), none_dic_apoyos, catalog_features.apoyos)

catalog_features.obj_gral_prog.replace('null', '', inplace=True)

# Dict features
#pub_current = pub_features[pub_features.anio.astype(int) > 2016].merge(cuaps_features, left_on='cd_programa', right_on='id_pub', how='outer')
#
#pub_history = pub_features[pub_features.anio.astype(int) <= 2016]
#pub_history['id_pub'] = 'null'
#pub_history['DGAE'] = 'null'
#pub_history['DGAIP'] = 'null'
#pub_history['cuaps_folio'] = 'null'
#pub_history['derechos_sociales'] = '[]'
#pub_history['apoyos'] = str(none_dic_apoyos)
#pub_history['criterios_focalizacion'] = '[]'
#
#catalog_features = pd.concat([pub_current, pub_history])

#catalog_features['nb_subp1'] = catalog_features.nb_subp1.apply(parse_list)

#################################
# Check for profiles
#################################

try:
    cuaps_sedesol = pd.read_sql_query('select * from semantic.cuaps_sedesol', con = engine)
except Exception as e:
    print(e)

catalog_profiles = catalog_features.merge(cuaps_sedesol, left_on='cd_programa',
        right_on='cve_programa', how='right')['cd_programa'].unique()
catalog_features['tiene_perfil'] = np.where(catalog_features.cd_programa.isin(catalog_profiles), 1, 0)

#################################
# Header test
#################################

selected_vars = ['cd_programa', 'id_pub', 'nb_programa', 'DGAE', 'DGAIP',
        'anio', 'obj_gral_prog', 'cuaps_folio', 'derechos_sociales',
        'nb_origen', 'nb_subp1', 'origen', 'padrones', 'apoyos',
        'criterios_focalizacion', 'tiene_perfil', 'nom_ent', 'cve_ent']

catalog_features = catalog_features[selected_vars]
catalog_features.fillna("null", inplace=True)

header_test = unittest.TestCase()
header_test.assertTrue(catalog_features.columns.values.tolist() == selected_vars)

#################################
# Look for missing values
#################################

missing_selected = ['cuaps_folio','cd_programa', 'nb_programa', 'anio',
    'obj_gral_prog', 'nom_ent', 'cve_ent']
missing_cuaps = catalog_features[(~catalog_features.cd_programa.isin(cuaps_programas_ids.id_pub)) & (catalog_features.cd_programa != 'null')][missing_selected].replace('null', '')
missing_pub = cuaps_features[(~cuaps_programas_ids.id_pub.isin(catalog_features.cd_programa.unique())) & (cuaps_programas_ids.id_pub != 'null')]

#################################
# Save files
#################################

catalog_features.to_csv('catalogo_cuaps_features_dict.csv', sep='|', index=False, encoding='utf-8', na_rep="")
missing_cuaps.to_csv('faltantes_cuaps.csv', sep='|', index=False, encoding='utf-8', na_rep="")

#Todo: reemplazar '' por "" desde Python
bucket = 'serverlesscatalogs'
s3_.Bucket(bucket).upload_file('catalogo_cuaps_features_dict.csv', 'catalogo_cuaps_features_dict.csv')
s3_.Bucket('dpa-plataforma-preventiva').upload_file('catalogo_cuaps_features_dict.csv', 'data/user/monizamudio/catalogo_cuaps_features_dict.csv')



