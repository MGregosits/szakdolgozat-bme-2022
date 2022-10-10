import sys
import pandas as pd
import os
import shutil
from os.path import join, isfile

#raw_data_folder = "/home/gmate/szakd/raw_data"
raw_data_folder = "./raw_data"

input_path = next((join(raw_data_folder, f) for f in os.listdir(raw_data_folder) if isfile(join(raw_data_folder, f))), 
 "default value here")

file_name = os.path.basename(input_path).split('/')[-1]

#preprocessed_data_folder = "/home/gmate/szakd/preprocessed_data"
preprocessed_data_folder = "./preprocessed_data"

output_path = preprocessed_data_folder + '/' + file_name

archive_folder = "./archived_raw_data"

archive_path = archive_folder + '/' + file_name

df = pd.read_csv(input_path, encoding='iso-8859-1', sep=';')

if( not df.empty):
    shutil.move(input_path, archive_path)

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

#os.remove(input_path)
