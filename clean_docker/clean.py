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

file_names_path = 'input/preprocessed_data/file_names.txt'

file_names_path_cle = 'input/cleaned_data/file_names.txt'

preprocessed_data_folder = "input/preprocessed_data"

cleaned_data_folder = "input/cleaned_data"

with client_hdfs.read(file_names_path) as reader:
    file_names_buffer = reader.read()
file_names_buffer = file_names_buffer.decode('iso-8859-1')
file_names_buffer = file_names_buffer.replace('\n','')

with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_buffer)
with open(file_names_path, 'r') as fin:
    file_names_data = fin.read().splitlines(True)

preprocessed_data_file_name = file_names_data[0]
preprocessed_data_file_name = preprocessed_data_file_name.replace('\n','')

with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_data[1:])

with open(file_names_path_cle, 'w') as fout:
    fout.writelines(file_names_data[0])

client_hdfs.delete(file_names_path)

with open(file_names_path) as reader, client_hdfs.write(file_names_path) as writer:
  for line in reader:
    writer.write(line)

input_path = preprocessed_data_folder + '/' + preprocessed_data_file_name

output_path = cleaned_data_folder + '/' + preprocessed_data_file_name

client_hdfs.download(input_path, input_path, overwrite=True)

reader = pd.read_csv(input_path, encoding='iso-8859-1' , sep=';', iterator=True, chunksize=2000000)


def cleaning_df(_df):
    _df = _df.drop(['temperature', 'door_status', 'heating', 'cooling', 'accuracy', 'ignition', 'engine_on', 'problem'],
     axis=1)
    return _df

chunks = []

for chunk in reader:
    chunk = cleaning_df(chunk)
    chunks.append(chunk)

df = pd.concat(chunks, axis=0)

print('Dataframe cleaning is DONE!')

df.to_csv(output_path,index=False, encoding='iso-8859-1', sep=';')


subprocess.run(["curl", "-i", "-X", "PUT", "-T", output_path, "http://adc3a714047a:9864/webhdfs/v1/user/root/input/cleaned_data/" + preprocessed_data_file_name + "?op=CREATE&namenoderpcaddress=namenode:9000&createflag=&createparent=true&overwrite=false"])

subprocess.run(["curl", "-i", "-X", "PUT", "-T", file_names_path_cle,"http://adc3a714047a:9864/webhdfs/v1/user/root/input/preprocessed_data/file_names.txt?op=APPEND&namenoderpcaddress=namenode:9000"])


os.remove(input_path)