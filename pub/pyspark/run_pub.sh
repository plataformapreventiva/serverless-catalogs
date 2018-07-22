#!/usr/bin/env bash
spark-submit --conf spark.executor.memory=7g --conf spark.executor.instances=8 --conf spark.executor.cores=5 --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/usr/bin/python3 --conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3 pub-clean-new2.py
#spark-submit pub-clean-new2.py
