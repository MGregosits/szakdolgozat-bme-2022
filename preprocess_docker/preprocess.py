import sys
import pandas as pd
import os
import subprocess
import pycurl
import certifi
from io import BytesIO, StringIO
from os.path import join, isfile, getsize

buffer1 = BytesIO()
c1 = pycurl.Curl()
c1.setopt(c1.URL, 'http://namenode:9870/webhdfs/v1/input/raw_data/file_names.txt?op=OPEN')
c1.setopt(c1.FOLLOWLOCATION, True)
c1.setopt(c1.WRITEDATA, buffer1)
c1.setopt(c1.CAINFO, certifi.where())
c1.perform()
c1.close()
file_names_buffer = buffer1.getvalue()
file_names_buffer = file_names_buffer.decode('iso-8859-1')

file_names_path = '/input/raw_data/file_names.txt'

with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_buffer)
with open(file_names_path, 'r') as fin:
    file_names_data = fin.read().splitlines(True)

raw_data_file_name = file_names_data[0]
raw_data_file_name = raw_data_file_name.replace('\n','')


with open(file_names_path, 'w') as fout:
    fout.writelines(file_names_data[1:])

c2 = pycurl.Curl()
c2.setopt(c2.URL, "http://namenode:9870/webhdfs/v1/input/raw_data/file_names.txt?op=DELETE")
c2.setopt(c2.FOLLOWLOCATION, True)
c2.setopt(c2.CUSTOMREQUEST, "DELETE")
c2.perform()
c2.close

subprocess.run(["curl", "-i", "-X", "PUT", "-T", file_names_path, "http://adc3a714047a:9864/webhdfs/v1/input/raw_data/file_names.txt?op=CREATE&namenoderpcaddress=namenode:9000&createflag=&createparent=true&overwrite=false"])

raw_data_url = "http://namenode:9870/webhdfs/v1/input/raw_data/" + raw_data_file_name + "?op=OPEN"


buffer2 = BytesIO()
c3 = pycurl.Curl()
c3.setopt(c3.URL, raw_data_url)
c3.setopt(c3.FOLLOWLOCATION, True)
c3.setopt(c3.WRITEDATA, buffer2)
c3.setopt(c3.CAINFO, certifi.where())
c3.perform()
c3.close()
#raw_data_buffer = buffer2.getvalue()
#raw_data_buffer = raw_data_buffer.decode('iso-8859-1')
buffer2.seek(0)

#raw_data_folder = "/home/gmate/szakd/raw_data"
raw_data_folder = "/input/raw_data"

input_path = raw_data_folder + '/' + raw_data_file_name


#preprocessed_data_folder = "/home/gmate/szakd/preprocessed_data"
preprocessed_data_folder = "/input/preprocessed_data"

output_path = preprocessed_data_folder + '/' + raw_data_file_name

#archive_folder = "/home/gmate/szakd/archived_raw_data"
#archive_folder = "./archived_raw_data"


df = pd.read_csv(buffer2, encoding='iso-8859-1' , sep=';')

#shutil.copy(input_path, archive_folder)

#os.remove(input_path)

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

#os.remove(input_path)


