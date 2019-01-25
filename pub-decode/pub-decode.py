#!/usr/bin/env python

from __future__ import print_function
import pdb
import argparse
import boto3
import boto
import os
import smart_open
import numpy as np
import string
from dotenv import find_dotenv, load_dotenv
import unicodedata
import gc

# Env Setup
load_dotenv(find_dotenv())
s3 = boto3.client('s3')

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

input_path = 's3://pub-etl/raw-txt/'
output_pathdec = 's3://pub-etl/decoded_txt/'

#output_path = 's3://pub-etl/glue_raw/'
#output_path2 = 's3://pub-etl/decoded_raw/'
#output_pathglue = 's3://pub-etl/glue_txt/'

years = np.arange(2016,2017)
# years = [2015]
n = 0
for year in years:
    FILE = "pub_"+str(year)+".txt"#0.gz"
    with smart_open.smart_open(output_pathdec+FILE, 'w') as gout:
            #smart_open.smart_open(output_path2+FILE, 'wb') as dout,\
            #smart_open.smart_open(output_pathglue+FILE, 'w') as hout:

        for line in smart_open.smart_open(input_path+FILE):
            n += 1
            print(n)
            if year == 2015:
                line = line.decode('cp1252')
            else:
                line = line.decode('Latin-1')
                #line = ''.join(char for char in line if ord(char) < 128)
                line = line.encode('ascii', errors='ignore').decode('utf-8')

            gout.write(line)
            line = None
            gc.collect()

            #line1 = remove_accents(line)
            #line1 = line1.replace('?','N').replace('\xbf','N')
            #line1 = ''.join(char for char in line1 if ord(char) < 128)
            #hout.write(line1)
            #line1 = line1.encode('ascii')
            #line2 = line.encode('utf8')
            #fout.write(line1)
            #dout.write(line2)
