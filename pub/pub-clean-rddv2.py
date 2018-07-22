#!/usr/bin/env python
from __future__ import print_function
import argparse
import boto3
import pdb
import re
import datetime
import os

import pyspark.sql.functions as func

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import col
from pyspark.sql.types import *

#os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
#os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

# create spark context and SQL context
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# Schema of raw table
D = {'cd_dependencia': 0,
     'nb_origen': 1,
     'id_admin': 2,
     'cd_programa': 3,
     'cd_padron': 4,
     'anio': 5,
     'tipo_beneficiario': 6,
     'nb_programa': 7,
     'nb_subprograma1': 8,
     'periodo': 9,
     'id_registro': 10,
     'id_hogar': 11,
     'id_cuis_ps': 12,
     'id_cuis_sedesol': 13,
     'fecha_levantamiento': 14,
     'id_persona': 15,
     'nb_primer_ap': 16,
     'nb_segundo_ap': 17,
     'nb_nombre': 18,
     'fh_nacimiento': 19,
     'cd_sexo': 20,
     'in_huella': 21,
     'in_iris': 22,
     'cd_institucion': 23,
     'cd_intraprograma': 24,
     'nb_subprograma': 25,
     'fecha_alta': 26,
     'cd_estatus_ben': 27,
     'cd_estatus_hog': 28,
     'cd_ent_pago': 29,
     'cd_mun_pago': 30,
     'cd_loc_pago': 31,
     'nb_periodo_corresp': 32,
     'cd_tipo_beneficio': 33,
     'cd_tipo_expedicion': 34,
     'in_titular': 35,
     'cd_parentesco': 36,
     'nu_beneficios': 37,
     'cd_beneficio': 38,
     'nu_imp_monetario': 39,
     'nu_mes_pago': 40,
     'cd_met_pago': 41,
     'id_agrupador': 42,
     'in_corresp': 43,
     'nom_loc': 44,
     'cve_loc': 45,
     'nom_mun': 46,
     'cve_mun': 47,
     'nom_ent': 48,
     'cve_ent': 49,
     'new_id': 50
    }

# schema of output table
SCHEMA_FULL = [
    'anio',
    'edad',
    'categoriaedad',
    'cdbeneficio',
    'cddependencia',
    'cdestatusben',
    'cdestatushog',
    'cdinstitucion',
    'cdintraprograma',
    'cdmetpago',
    'cdpadron',
    'cdparentesco',
    'cdprograma',
    'cdsexo',
    'cdtipobeneficio',
    'cdtipoexpedicion',
    'cveent',
    'cveentpago',
    'cveloc',
    'cvelocpago',
    'cvemuni',
    'cvemunipago',
    'fechaalta',
    'fechalevantamiento',
    'fechanacimiento',
    'idadmin',
    'idagrupador',
    'idcuisps',
    'idcuissedesol',
    'idhogar',
    'idpersona',
    'idregistro',
    'incorresp',
    'inhuella',
    'iniris',
    'intitular',
    'mescorresp',
    'nbnombre',
    'nbperiodocorresp',
    'nbprimerap',
    'nbprograma',
    'nbsegundoap',
    'nbsubprograma',
    'nbsubprograma1',
    'newid',
    'noment',
    'nomloc',
    'nommun',
    'nubeneficios',
    'nuimpmonetario',
    'numespago',
    'origen',
    'periodo',
    'tipobeneficiario'
#    'programatipo',
#    'iduni',
#    'nombresubp1',
#    'nombreprograma',
#    'nb_dependencia',
#    'nb_depen_corto'
         ]

# schema of output table for publications
SCHEMA_PUBLICATION = []


def clean_pub_line(line):
    """
    This class cleans each row of the RDD table
    """
    # splits the elements of the row
    elems = line.split("|")

    # Change variables:
    # clean year:
    anio = clean_integer(elems[D['anio']])
    # clean months:
    nu_mes_pago = clean_month(elems[D['nu_mes_pago']])
    mes_corresp = corresp_month(elems[D['periodo']])
    # clean age:
    age = to_age(elems[D['fh_nacimiento']], anio, mes_corresp)
    age_category = gen_age_category(age)
    # clean name and lastnames:
    ap_pat = clean_name(elems[D['nb_primer_ap']], age)
    ap_mat = clean_name(elems[D['nb_segundo_ap']], age)
    nombre = clean_name(elems[D['nb_nombre']], age)
    # clean gender:
    gender = clean_gender(elems[D['cd_sexo']])
    # clean origin of program:
    origin = clean_origin(elems[D['nb_origen']])
    # clean money payed
    nu_imp_monetario = clean_integer(elems[D['nu_imp_monetario']])
    # Payment location
    cve_edo_pago = clean_edo(elems[D['cd_ent_pago']])
    cve_muni_pago = clean_muni(elems[D['cd_mun_pago']],
                               cve_edo_pago)
    cve_loc_pago = clean_loc(elems[D['cd_loc_pago']])
    # Person location
    cve_edo = clean_edo(elems[D['cve_ent']])
    cve_muni = clean_muni(elems[D['cve_mun']], cve_edo)
    cve_loc = clean_loc(elems[D['cve_loc']])
    # clean type of benefit
    nombre_tipo_beneficio = name_benefit(elems[D['cd_tipo_beneficio']])

    ## Remove cloumns
    remove_cols = ['nb_origen', 'anio', 'nb_primer_ap', 'nb_segundo_ap',
                   'nb_nombre', 'cd_sexo', 'cd_ent_pago', 'cd_mun_pago',
                    'cd_loc_pago', 'nu_mes_pago', 'cve_loc', 'cve_mun',
                    'cve_ent', 'nu_imp_monetario']
    # New columns
    new_cols = [origin, anio, ap_pat, ap_mat,
                nombre, gender, cve_edo_pago,
                cve_muni_pago, cve_loc_pago,
                nu_mes_pago, mes_corresp, age, age_category,
                nu_imp_monetario, cve_edo, cve_muni, cve_loc,
                nombre_tipo_beneficio]
    new_line = gen_new_clean_line(elems, remove_cols, new_cols)
    return new_line


def trim(x):
    try:
        return x.strip()
    except:
        return ""

def clean_string(x):
    if isinstance(x, str):
        return x.replace('"', '')
    else:
        return x

def clean_integer(raw_int):
    raw_int = clean_string(raw_int)
    try:
        clean_int = int(raw_int)
    except:
        clean_int = None
    return clean_int

def clean_name(name, age):
    name = clean_string(name)
    try:
        if age < 18:
            return None
        else:
            return name
    except:
        return name

def to_age(birthdate, anio, mes_corresp):
    birthdate = str(clean_string(birthdate))
    try:
        today = datetime.date(year=anio, day=1, month=mes_corresp)
        born = datetime.datetime.strptime(birthdate, '%Y%m%d')
        age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except:
        age = None
    return age

def gen_age_category(age):
    try:
        if age > 60:
            category = 'Adulto Mayor'
        elif age <= 11:
            category = 'Infante'
        elif (age >= 12) & (age <= 18):
            category = 'Adolescentes'
        elif (age >= 19) & (age <= 29):
            category = 'Jóvenes'
        elif (age >= 30) & (age <= 60):
            category = 'Adultos'
    except:
        category = None
    return category

def clean_muni(muni, edo):
    muni = clean_string(muni)
    try:
        edo_int = int(edo)
        muni_int = int(muni)
    except:
        return None
    if (edo_int <= 32) & (edo_int > 0) & (muni_int < 999) & (muni_int > 0):
        cve_muni = "{ent}{mun}".format(ent=str(edo_int).zfill(2),
                                       mun=str(muni_int).zfill(4))
        return cve_muni

def clean_loc(loc):
    loc = clean_string(loc)
    try:
        loc_int = int(loc)
    except:
        return None
    if (loc_int > 9999) & (loc_int > 0):
        cve_loc = str(loc_int).zfill(4)
        return cve_loc

def clean_edo(edo):
    edo = clean_string(edo)
    try:
        edo_int = int(edo)
    except:
        return None
    if (edo_int <= 32) & (edo_int > 0):
        cve_edo = str(edo_int).zfill(2)
        return cve_edo

def clean_gender(gender):
    try:
        if re.search('H', gender):
            gen = 'H'
        elif re.search('M', gender):
            gen = 'M'
        else:
            gen = None
    except:
        gen = None
    return gen

def clean_origin(origin):
    try:
        if re.search('F', origin):
            clean_origin = 'F'
        elif re.search('E', origin):
            clean_origin = 'E'
        else:
            clean_origin = None
    except:
        clean_origin = None
    return clean_origin

def clean_month(raw_month):
    raw_month = clean_string(raw_month)
    try:
        clean_month = int(raw_month)
    except:
        return None
    if (clean_month >= 1) & (clean_month <= 12):
        return clean_month

def corresp_month(periodo):
    periodo = clean_string(periodo)
    if periodo:
        corresp_month = periodo[2]
        try:
            clean_month = int(corresp_month)
        except:
            if corresp_month.lower() == 'a':
                clean_month = 10
            elif corresp_month.lower() == 'b':
                clean_month = 11
            elif corresp_month.lower() == 'c':
                clean_month = 12
            else:
                clean_month = None
        return clean_month

def name_benefit(benefit):
    benefit = clean_string(benefit)
    try:
        benefit_int = int(benefit)
        if benefit_int == 1:
            benefit_name = 'monetario'
        elif benefit_int == 2:
            benefit_name = 'especie'
        elif benefit_int == 3:
            benefit_name = 'servicio'
        elif benefit_int == 4:
            benefit_name = 'mixto'
        elif benefit_int == 5:
            benefit_name = 'subsidiado'
        elif benefit_int == 6:
            benefit_name = 'indirecto'
        else:
            benefit_name = None
    except:
        benefit_name = None
    return benefit_name

def gen_new_clean_line(elems, remove_cols, new_cols):
    old_cols = [elems[D[x]] for x in sorted(D.keys())
                if x not in remove_cols]
    clean_cols = old_cols + new_cols
    return "|".join([str(x) if x else '' for x in clean_cols])


def read_pub(year, input_path, size=1):
    # read a raw text file from s3
    raw_data = sc.textFile(input_path + "pub_{0}.txt.gz".format(year))
    # Remove header
    raw_data = raw_data.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])
    # Make sample
    #raw_data = raw_data.sample(False, size, 1234)
    return raw_data


def read_catalog(catalogo_file):
    customSchema = StructType([
        StructField("extra", StringType(), True),
        StructField("lcddependencia", StringType(), True),
        StructField("nborigen", StringType(), True),
        StructField("idadmin", IntegerType(), True),
        StructField("cdprograma", StringType(), True),
        StructField("cdpadron", StringType(), True),
        StructField("anio", IntegerType(), True),
        StructField("nombreprograma", StringType(), True),
        StructField("nombresubp1", StringType(), True),
        StructField("origen", StringType(), True),
        StructField("iduni", StringType(), True),
        StructField("origen_dep", StringType(), True),
        StructField("nb_dependencia", StringType(), True),
        StructField("nb_depen_corto", StringType(), True),
        StructField("sector", StringType(), True)]
    )

    df = sqlContext.read.format('com.databricks.spark.csv') \
            .option("header", 'true') \
            .option("delimiter", "|") \
            .load(catalogo_file, schema = customSchema)
    return df


def store_partitions(df, variables, input_path, output_path):
    df.select(SCHEMA_FULL).write.mode('overwrite').partitionBy(*variables).parquet(output_path)


def accept_int(x):
    if x:
        return int(x)
    else:
        return None

if __name__ == "__main__":
    # Read pub
    year = '2017'
    input_path = 's3a://pub-raw/new_raw/'
    #output_path = 'hdfs:///home/hadoop/publicaciones-sedesol/anio={}/'.format(year)
    output_path = 's3a://publicaciones-sedesol/pub-new/anio={}/'.format(year)
    variables = ['numespago']

    # Read raw pub
    raw_sc = read_pub(year, input_path, 0.0001)
    # clean pub
    clean_sc = raw_sc.map(clean_pub_line)
    parts = clean_sc.map(lambda l: l.split("|"))
    # Make it sql context
    clean_data = parts.map(lambda p: Row(
        cdbeneficio=accept_int(p[0]),
        cddependencia=accept_int(p[1]),
        cdestatusben=accept_int(p[2]),
        cdestatushog=p[3],
        cdinstitucion=p[4],
        cdintraprograma=p[5],
        cdmetpago=accept_int(p[6]),
        cdpadron=p[7],
        cdparentesco=p[8],
        cdprograma=p[9],
        cdtipobeneficio=accept_int(p[10]),
        cdtipoexpedicion=p[11],
        fechaalta=p[12],
        fechalevantamiento=p[13],
        fechanacimiento=p[14],
        idadmin=p[15],
        idagrupador=p[16],
        idcuisps=p[17],
        idcuissedesol=p[18],
        idhogar=p[19],
        idpersona=p[20],
        idregistro=p[21],
        incorresp=p[22],
        inhuella=accept_int(p[23]),
        iniris=accept_int(p[24]),
        intitular=accept_int(p[25]),
        nbperiodocorresp=p[26],
        nbprograma=p[27],
        nbsubprograma=p[28],
        nbsubprograma1=p[29],
        newid=p[30],
        noment=p[31],
        nomloc=p[32],
        nommun=p[33],
        nubeneficios=p[34],
        periodo=p[35],
        tipobeneficiario=p[36],
        origen=p[37],
        anio=int(p[38]),
        nbprimerap=p[39],
        nbsegundoap=p[40],
        nbnombre=p[41],
        cdsexo=p[42],
        cveentpago=p[43],
        cvemunipago=p[44],
        cvelocpago=p[45],
        numespago=accept_int(p[46]),
        mescorresp=accept_int(p[47]),
        edad=p[48],
        categoriaedad=p[49],
        nuimpmonetario=accept_int(p[50]),
        cveent=p[51],
        cvemuni=p[52],
        cveloc=p[53]
        )
    )
    clean_df = sqlContext.createDataFrame(clean_data)

    # Add new columns for joins
    function1 = func.udf(lambda col1, col2, col3 : '{0}_{1}_{2}'.format(col1, col2, col3), StringType())
    function2 = func.udf(lambda col1, col2, col3, col4, col5 : '{0}_{1}_{2}_{3}_{4}'.format(col1, col2, col3, col4, col5), StringType())

    #clean_df = clean_df.withColumn('programatipo', function1(col('cdprograma'), col('cdpadron'), col('cdtipobeneficio')))
    #clean_df = clean_df.withColumn('iduni', function2(col('origen'),
    #                                                  col('cddependencia'),
    #                                                  col('cdprograma'),
    #                                                  col('cdpadron'),
    #                                                  col('anio')))

    # Read catalogo
    #catalogo_file = 's3://pub-raw/diccionarios/catalogo_programas.csv'
    #catalogo = read_catalog(catalogo_file)
    #catalogo_columns = ['iduni', 'nombresubp1','nombreprograma','nb_dependencia', 'nb_depen_corto']
    #catalogo = catalogo.select(*catalogo_columns).filter(catalogo.anio == year)

    # Join with catalogo
    #clean_df = clean_df.join(func.broadcast(catalogo),
    #        clean_df.iduni == catalogo.iduni, 'left').drop(catalogo.iduni)
    # Store with partitions
    store_partitions(clean_df, variables, input_path, output_path)
