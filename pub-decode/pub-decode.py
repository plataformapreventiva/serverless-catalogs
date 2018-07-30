#!/usr/bin/env python

from __future__ import print_function
import argparse
import boto3
import boto
import os
import smart_open
import numpy as np
import string

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])

output_path = 's3://pub-raw/glue_raw/'
output_path2 = 's3://pub-raw/decoded_raw/'
input_path = 's3://pub-raw/new_raw/'
output_pathdec = 's3://pub-raw/decoded_txt/'
output_pathglue = 's3://pub-raw/glue_txt/'

years = np.arange(2011,2017)
for year in years:
    FILE = "pub_"+str(year)+".txt.gz"
    with smart_open.smart_open(output_path+FILE, 'wb') as fout,\
            smart_open.smart_open(output_path2+FILE, 'wb') as dout,\
            smart_open.smart_open(output_pathdec+FILE, 'w') as gout\
            smart_open.smart_open(output_pathglue+FILE, 'w') as hout:
        for line in smart_open.smart_open(input_path+FILE):
            if year < 2015:
                line = line.decode('cp1252')
            else:
                line = line.decode('Latin-1')
            gout.write(line)
            line1 = remove_accents(line)
            line1 = line1.replace('?','N').replace('\xbf','N')
            line1 = ''.join(char for char in line1 if ord(char) < 128)
            hout.write(line1)
            line1 = line1.encode('ascii')
            line2 = line.encode('utf8')
            fout.write(line1)
            dout.write(line2)
