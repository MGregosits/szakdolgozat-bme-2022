import sys
import pandas as pd
import os
import subprocess
import csv
import pycurl
from hdfs import InsecureClient
import certifi
from io import BytesIO, StringIO
from os.path import join, isfile, getsize


client_hdfs = InsecureClient('http://namenode:9870', user='root')

file_names_path = 'input/raw_data/file_names.txt'

with client_hdfs.read(file_names_path) as reader:
    file_names_buffer = reader.read()
file_names_buffer = file_names_buffer.decode('iso-8859-1')
file_names_buffer = file_names_buffer.replace('\n','')



with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_buffer)
with open(file_names_path, 'r') as fin:
    file_names_data = fin.read().splitlines(True)

raw_data_file_name = file_names_data[0]
raw_data_file_name = raw_data_file_name.replace('\n','')

with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_data[1:])

client_hdfs.delete(file_names_path)

with open(file_names_path) as reader, client_hdfs.write(file_names_path) as writer:
  for line in reader:
    writer.write(line)

os.remove(file_names_path)

raw_data_folder = "input/raw_data"

input_path = raw_data_folder + '/' + raw_data_file_name

preprocessed_data_folder = "input/preprocessed_data"

output_path = preprocessed_data_folder + '/' + raw_data_file_name

#archive_folder = "/home/gmate/szakd/archived_raw_data"
#archive_folder = "./archived_raw_data"

client_hdfs.download(input_path, input_path, overwrite=True)