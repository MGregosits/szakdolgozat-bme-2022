import pandas as pd
import os
from os.path import join, isfile
import shutil

preprocessed_data_folder = "./preprocessed_data"

input_path = next((join(preprocessed_data_folder, f) for f in os.listdir(preprocessed_data_folder)
 if isfile(join(preprocessed_data_folder, f))), "default value here")

file_name = os.path.basename(input_path).split('/')[-1]

stripped_data_folder = "./stripped_data"

output_path = stripped_data_folder + '/' + file_name

archive_folder = "./archived_preprocessed_data"

archive_path = archive_folder + '/' + file_name

df = pd.read_csv(input_path, encoding='iso-8859-1', sep=';')

if( not df.empty):
    shutil.move(input_path, archive_path)

def stripping_df(_df):
    _df = _df.drop(['temperature', 'door_status', 'heating', 'cooling', 'accuracy', 'ignition', 'engine_on', 'problem'],
     axis=1)
    return _df

df = stripping_df(df)

print('Dataframe stripping is DONE!')

df.to_csv(output_path,index=False, encoding='iso-8859-1', sep=';')

#os.remove(input_path)