SELECT anio,
       numespago,
       cveent,
       cvemuni,
       origin,
       cddependencia,
       nbdependencia,
       cdprograma,
       nbprograma
FROM pub_nominal
GROUP BY anio, numespago, cveent, cvemuni, origin, cddependencia, nbdependencia, cdprograma, nbprograma;
