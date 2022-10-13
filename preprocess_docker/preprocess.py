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


raw_data_folder = "input/raw_data"

input_path = raw_data_folder + '/' + raw_data_file_name

preprocessed_data_folder = "input/preprocessed_data"

output_path = preprocessed_data_folder + '/' + raw_data_file_name

#archive_folder = "/home/gmate/szakd/archived_raw_data"
#archive_folder = "./archived_raw_data"

client_hdfs.download(input_path, input_path, overwrite=True)

df = pd.read_csv(input_path, encoding='iso-8859-1' , sep=';')
df.head()


def preprocess_df(_df):
    unique_run_ids = _df['run_id'].unique()
    number_of_kept = 0
    index_to_keep = []
    for id_iterator in unique_run_ids:
        tmp_df = _df.loc[_df['run_id'] == id_iterator, ['run_id', 'time_stamp','last_station_number']]
        if tmp_df['last_station_number'].nunique() > 6:
            index_to_keep.append(tmp_df.index.to_list())
            number_of_kept += 1
    print (str(number_of_kept) + ' has been kept')

    flat_index_to_keep = [item for sublist in index_to_keep for item in sublist]
    _df = _df.filter(flat_index_to_keep, axis='index')
    _df['time_stamp'] = pd.to_datetime(_df['time_stamp'])
    return _df

df = preprocess_df(df)

print('Preprocessing is DONE!')

df.to_csv(output_path,index=False, encoding='iso-8859-1', sep=';')

subprocess.run(["curl", "-i", "-X", "PUT", "-T", output_path, "http://adc3a714047a:9864/webhdfs/v1/input/preprocessed_data/" + raw_data_file_name + "?op=CREATE&namenoderpcaddress=namenode:9000&createflag=&createparent=true&overwrite=false"])

os.remove(input_path)

