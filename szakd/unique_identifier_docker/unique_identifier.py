import pandas as pd
import os
from os.path import join, isfile


stripped_data_folder = "./stripped_data"

input_path = next((join(stripped_data_folder, f) for f in os.listdir(stripped_data_folder)
 if isfile(join(stripped_data_folder, f))), "default value here")

file_name = os.path.basename(input_path).split('/')[-1]

uniquely_identified_data_folder = "./uniquely_identified_data"

output_path = uniquely_identified_data_folder + '/' + file_name

archive_folder = "./archived_stripped_data"

archive_path = archive_folder + '/' + file_name


df = pd.read_csv(input_path, encoding='iso-8859-1', sep=';')

if( not df.empty):
    shutil.move(input_path, archive_path)

def giveunique(_df):
    station_i = []
    last_station_list = _df['last_station_number'].to_list()
    run_nr = 0
    station_i.append(0)
    for i in range(1,len(last_station_list)):
        if  last_station_list[i] < last_station_list[i-1]:
            run_nr = run_nr+1
        station_i.append(run_nr)
    return station_i


def create_tmp_i(_df):
    tmp_i_df = pd.DataFrame(columns=['tmp_i'])
    unique_run_ids = _df['run_id'].unique()

    for i in unique_run_ids:
        tmp_df = _df.loc[_df['run_id'] == i]
        tmp_df = tmp_df.sort_values(['time_stamp', 'last_station_number'], ascending=[True, True])
        tmp_df['tmp_i'] = giveunique(tmp_df)
        tmp_i_df = pd.concat([tmp_i_df, tmp_df[['tmp_i']]])
    
    return tmp_i_df
    

def giveidentifier(_df):
    _df = _df.join(create_tmp_i(_df), how='left')
    _df['run_id_string'] = _df['run_id'].astype(int).astype(str)
    _df['tmp_i_string'] = _df["tmp_i"].astype(str)
    _df['unique_identifier'] = _df[['run_id_string', 'tmp_i_string']].agg('--'.join, axis=1)
    _df = _df.drop(['tmp_i', 'run_id_string', 'tmp_i_string'], axis=1)
    return _df

df = giveidentifier(df)

print('Giving unique identifiers is DONE!')

df.to_csv(output_path,index=False, encoding='iso-8859-1', sep=';')

#os.remove(input_path)