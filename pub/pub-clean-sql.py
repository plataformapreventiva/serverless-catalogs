from __future__ import print_function
import argparse
import boto3
import re
import pdb
import datetime
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.functions import col, udf, broadcast
from pyspark.sql.types import *

# Define context and properties

#SparkContext.stop(sc)
#SparkSession._instantiatedContext = None
SparkContext.setSystemProperty('spark.executor.cores','4')
SparkContext.setSystemProperty('spark.driver.maxResultSize', '10g')
SparkContext.setSystemProperty("spark.executor.memory", "20g")
SparkContext.setSystemProperty("spark.executor.memoryOverhead", "8g")
SparkContext.setSystemProperty("spark.driver.memoryOverhead", "8g")
SparkContext.setSystemProperty("spark.driver.memory", "25g")
#conf=SparkConf().setMaster("local").setAppName("Basic")
# spark = SparkSession.builder.appName('Basic').getOrCreate()
#sc=SparkContext(conf=conf)
sc = SparkContext.getOrCreate()
sc._conf.getAll()
sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")

# schema of output table
SCHEMA_FULL = [
    'anio',
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
    'fhnacimiento',
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
    'tipobeneficiario',
    'programatipo',
    'iduni',
    'nombresubp1',
    'nombreprograma',
    'nbdependencia',
    'nbdepencorto'
         ]

# schema of output table for publications
SCHEMA_PUBLICATION = []

new_id_udf1 = udf(lambda col1, col2, col3 : '{0}_{1}_{2}'.format(col1, col2, col3), StringType())

new_id_udf2 = udf(lambda col1, col2, col3, col4, col5 : '{0}_{1}_{2}_{3}_{4}'.format(col1, col2, col3, col4, col5), StringType())

def make_new_id(list_names):
    if len(list_names) > 0:
        new_id = "_".join(list_names)
        return new_id
    else:
        return ""

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

clean_string_udf = udf(lambda z: clean_string(z), StringType())

def clean_integer(raw_int):
    raw_int = clean_string(raw_int)
    try:
        clean_int = int(raw_int)
    except:
        clean_int = None
    return clean_int

clean_integer_udf = udf(lambda z: clean_integer(z), IntegerType())

def clean_name(name, age):
    try:
        if age < 18:
            return None
        else:
            return name
    except:
        return name

clean_name_udf = udf(lambda y,z: clean_name(y,z), StringType())

def to_age(birthdate, year, mes_corresp):
    birthdate = str(clean_string(birthdate))
    try:
        today = datetime.date(year=year, day=1, month=mes_corresp)
        born = datetime.datetime.strptime(birthdate, '%Y%m%d')
        age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    except:
        age = None
    return age

to_age_udf = udf(lambda x,y,z: to_age(x,y,z), IntegerType())

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

gen_age_category_udf = udf(lambda z: gen_age_category(z), StringType())

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

clean_muni_udf = udf(lambda y,z: clean_muni(y,z), StringType())

def clean_loc(loc):
    loc = clean_string(loc)
    try:
        loc_int = int(loc)
    except:
        return None
    if (loc_int > 9999) & (loc_int > 0):
        cve_loc = str(loc_int).zfill(4)
        return cve_loc

clean_loc_udf = udf(lambda z: clean_loc(z), StringType())

def clean_edo(edo):
    edo = clean_string(edo)
    try:
        edo_int = int(edo)
    except:
        return None
    if (edo_int <= 32) & (edo_int > 0):
        cve_edo = str(edo_int).zfill(2)
        return cve_edo

clean_edo_udf = udf(lambda z: clean_edo(z), StringType())

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

clean_gender_udf = udf(lambda z: clean_gender(z), StringType())

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

clean_origin_udf = udf(lambda z: clean_origin(z), StringType())

def clean_month(raw_month):
    raw_month = clean_string(raw_month)
    try:
        clean_month = int(raw_month)
    except:
        return None
    if (clean_month >= 1) & (clean_month <= 12):
        return clean_month

clean_month_udf = udf(lambda z: clean_month(z), IntegerType())

def corresp_month(periodo):
    periodo = clean_string(periodo)
    if periodo:
        mes_corresp = periodo[2]
        try:
            clean_month = int(mes_corresp)
        except:
            if mes_corresp.lower() == 'a':
                clean_month = 10
            elif mes_corresp.lower() == 'b':
                clean_month = 11
            elif mes_corresp.lower() == 'c':
                clean_month = 12
            else:
                clean_month = None
        return clean_month

corresp_month_udf = udf(lambda z: corresp_month(z), IntegerType())

def name_benefit(benefit_type):
    benefit = clean_string(benefit_type)
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

name_benefit_udf = udf(lambda z: name_benefit(z), StringType())

def read_pub(year, input_path, size=1):
    pub_file = input_path + "pub_{0}.txt.gz".format(year)
    customSchema = StructType([
        StructField("cddependencia", StringType(), True),
        StructField("nborigen", StringType(), True),
        StructField("idadmin", StringType(), True),
        StructField("cdprograma", StringType(), True),
        StructField("cdpadron", StringType(), True),
        StructField("anio", StringType(), True),
        StructField("tipobeneficiario", StringType(), True),
        StructField("nbprograma", StringType(), True),
        StructField("nbsubprograma1", StringType(), True),
        StructField("periodo", StringType(), True),
        StructField("idregistro", StringType(), True),
        StructField("idhogar", StringType(), True),
        StructField("idcuisps", StringType(), True),
        StructField("idcuissedesol", StringType(), True),
        StructField("fechalevantamiento", StringType(), True),
        StructField("idpersona", StringType(), True),
        StructField("nbprimerap", StringType(), True),
        StructField("nbsegundoap", StringType(), True),
        StructField("nbnombre", StringType(), True),
        StructField("fhnacimiento", StringType(), True),
        StructField("cdsexo", StringType(), True),
        StructField("inhuella", StringType(), True),
        StructField("iniris", StringType(), True),
        StructField("cdinstitucion", StringType(), True),
        StructField("cdintraprograma", StringType(), True),
        StructField("nbsubprograma", StringType(), True),
        StructField("fechaalta", StringType(), True),
        StructField("cdestatusben", StringType(), True),
        StructField("cdestatushog", StringType(), True),
        StructField("cdentpago", StringType(), True),
        StructField("cdmunpago", StringType(), True),
        StructField("cdlocpago", StringType(), True),
        StructField("nbperiodocorresp", StringType(), True),
        StructField("cdtipobeneficio", StringType(), True),
        StructField("cdtipoexpedicion", StringType(), True),
        StructField("intitular", StringType(), True),
        StructField("cdparentesco", StringType(), True),
        StructField("nubeneficios", StringType(), True),
        StructField("cdbeneficio", StringType(), True),
        StructField("nuimpmonetario", StringType(), True),
        StructField("numespago", StringType(), True),
        StructField("cdmetpago", StringType(), True),
        StructField("idagrupador", StringType(), True),
        StructField("incorresp", StringType(), True),
        StructField("nomloc", StringType(), True),
        StructField("cveloc", StringType(), True),
        StructField("nommun", StringType(), True),
        StructField("cvemun", StringType(), True),
        StructField("noment", StringType(), True),
        StructField("cveent", StringType(), True),
        StructField("newid", StringType(), True)
    ])
    # read a raw text file from s3
    raw_data = sqlContext.read.format('com.databricks.spark.csv') \
            .option("header", 'true') \
            .option("delimiter", "|") \
            .load(pub_file, schema = customSchema)
    # Make sample
    #raw_data = raw_data.sample(False, size, 42)
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
        StructField("anio", StringType(), True),
        StructField("nombreprograma", StringType(), True),
        StructField("nombresubp1", StringType(), True),
        StructField("origen", StringType(), True),
        StructField("iduni", StringType(), True),
        StructField("origendep", StringType(), True),
        StructField("nbdependencia", StringType(), True),
        StructField("nbdepencorto", StringType(), True),
        StructField("sector", StringType(), True)]
    )
    df = sqlContext.read.format('com.databricks.spark.csv') \
            .option("header", 'true') \
            .option("delimiter", "|") \
            .load(catalogo_file, schema = customSchema)
    return df

if __name__ == "__main__":
    # Read pub
    year = '2017'
    input_path = 's3://pub-raw/new_raw/'
    output_path = 's3a://publicaciones-sedesol/pub-new/anio={}/'.format(year)
    variables = ['numespago']
    # Read raw pub
    print("reading")
    ## Test con SparkSession
    raw_data = read_pub(year, input_path, 0.0001)
    print("done reading")
    # clean pub
    print("start cleaning")
    # clean year:
    raw_data = raw_data.withColumn('anio', clean_integer_udf(col('anio')))
    # clean months:
    raw_data = raw_data.withColumn('numespago', clean_month_udf(col('numespago')))
    raw_data = raw_data.withColumn('mescorresp', corresp_month_udf(col('periodo')))
    # clean age
    raw_data = raw_data.withColumn('age', to_age_udf(col('fhnacimiento'),
                                  col('anio'),
                                  col('mescorresp')))
    raw_data = raw_data.withColumn('categoriaedad', gen_age_category_udf(col('age')))
    # clean name and lastnames
    raw_data = raw_data.withColumn('nbprimerap', clean_name_udf(col('nbprimerap'),col('age')))
    raw_data = raw_data.withColumn('nbsegundoap', clean_name_udf(col('nbsegundoap'),col('age')))
    raw_data = raw_data.withColumn('nbnombre', clean_name_udf(col('nbnombre'),col('age')))
    # clean_gender
    raw_data = raw_data.withColumn('cdsexo', clean_gender_udf(col('cdsexo')))
    # clean origin
    raw_data = raw_data.withColumn('origen', clean_origin_udf(col('nborigen')))
    # clean money
    raw_data = raw_data.withColumn('nuimpmonetario', clean_integer_udf(col('nuimpmonetario')))
    # paymente location
    raw_data = raw_data.withColumn('cveentpago', clean_edo_udf(col('cdentpago')))
    raw_data = raw_data.withColumn('cvemunipago',
    clean_muni_udf(col('cdmunpago'),col('cveentpago')))
    raw_data = raw_data.withColumn('cvelocpago', clean_loc_udf(col('cdlocpago')))
    # Person location
    raw_data = raw_data.withColumn('cveedo', clean_edo_udf(col('cveent')))
    raw_data = raw_data.withColumn('cvemuni', clean_muni_udf(col('cveent'), col('cvemun')))
    raw_data = raw_data.withColumn('cveloc', clean_loc_udf(col('cveloc')))
    # clean type of benefit
    raw_data = raw_data.withColumn('nombretipobeneficio',
    name_benefit_udf(col('cdtipobeneficio')))
    # Add new columns for join
    raw_data = raw_data.withColumn('programatipo',
    new_id_udf1(col('cdprograma'),col('cdpadron'),col('cdtipobeneficio')))
    raw_data = raw_data.withColumn('iduni',
        new_id_udf2(col('origen'),col('cddependencia'),col('cdprograma'),col('cdpadron'),col('anio')))
    print("done")
    # Read catalogo
    catalogo_file = 's3://pub-raw/diccionarios/catalogo_programas.csv'
    catalogo = read_catalog(catalogo_file)
    catalogo_columns = ['anio', 'iduni', 'nombresubp1','nombreprograma','nbdependencia', 'nbdepencorto']
    print("filtering")
    catalogo = catalogo.select(*catalogo_columns).filter(catalogo.anio == year)
    catalogo = catalogo.withColumnRenamed("anio", "anio_catalogo")
    # Join with catalogo
    print("join")
    raw_data = raw_data.join(broadcast(catalogo),
        raw_data.iduni == catalogo.iduni, 'left').drop(catalogo.iduni)
    # Store with partitions
    print("saving")
    raw_data.select(SCHEMA_FULL).write.mode('overwrite').partitionBy(*variables).parquet(output_path)
