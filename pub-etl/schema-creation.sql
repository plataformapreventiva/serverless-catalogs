--
-- SEDESOL : DataLab
-- Athena - PUB table definition
--


-- Cleaned table
CREATE EXTERNAL TABLE IF NOT EXISTS athena_pub.pub_cleaned (
  categoriaedad STRING,
  cdbeneficio STRING,
  cddependencia STRING,
  cdestatusben STRING,
  cdestatushog STRING,
  cdinstitucion STRING,
  cdintraprograma STRING,
  cdmetpago STRING,
  cdpadron STRING,
  cdparentesco STRING,
  cdprograma STRING,
  cdsexo STRING,
  cdtipobeneficio STRING,
  nombretipobeneficio STRING,
  cdtipoexpedicion STRING,
  cveent STRING,
  cveentpago STRING,
  cveloc STRING,
  cvelocpago STRING,
  cvemuni STRING,
  cvemunipago STRING,
  fechaalta STRING,
  fechalevantamiento STRING,
  fhnacimiento STRING,
  idadmin STRING,
  idagrupador STRING,
  idcuisps STRING,
  idcuissedesol STRING,
  idhogar STRING,
  idpersona STRING,
  idregistro STRING,
  incorresp STRING,
  inhuella STRING,
  iniris STRING,
  intitular STRING,
  mescorresp INT,
  nbnombre STRING,
  nbperiodocorresp STRING,
  nbprimerap STRING,
  nbprograma STRING,
  nbsegundoap STRING,
  nbsubprograma STRING,
  nbsubprograma1 STRING,
  newid STRING,
  noment STRING,
  nomloc STRING,
  nommun STRING,
  nubeneficios STRING,
  nuimpmonetario INT,
  origen STRING,
  periodo STRING,
  tipobeneficiario STRING,
  programatipo STRING,
  iduni STRING,
  nombresubp1 STRING,
  nombreprograma STRING,
  nbdependencia STRING,
  nbdepencorto STRING)
  PARTITIONED BY (anio STRING, numespago STRING)
  STORED AS PARQUET
  LOCATION 's3://publicaciones-sedesol/pub-cleaned/';

-- Re create partitions
msck repair table athena_pub.pub_cleaned;

--Publication table
CREATE EXTERNAL TABLE IF NOT EXISTS athena_pub.pub_public (
    categoriaedad  STRING,
    cdbeneficio  STRING,
    cddependencia  STRING,
    cdinstitucion  STRING,
    cdpadron  STRING,
    cdprograma  STRING,
    cdsexo  STRING,
    cdtipobeneficio  STRING,
    nombretipobeneficio STRING,
    cveent  STRING,
    cveentpago  STRING,
    cvemuni  STRING,
    cvemunipago  STRING,
    mescorresp  INT,
    nbnombre  STRING,
    nbprimerap  STRING,
    nbprograma  STRING,
    nbsegundoap  STRING,
    newid  STRING,
    noment  STRING,
    nommun  STRING,
    nubeneficios  STRING,
    nuimpmonetario  INT,
    origen  STRING,
    periodo  STRING,
    programatipo  STRING,
    nombresubp1  STRING,
    nombreprograma  STRING,
    nbdependencia  STRING,
    nbdepencorto STRING )
  PARTITIONED BY (anio STRING, numespago STRING)
  STORED AS PARQUET
  LOCATION 's3://publicaciones-sedesol/pub-publicacion/';

-- Re create partitions
msck repair table athena_pub.pub_public
