import pandas as pd
import json
import requests
import ast
import shutil
import os
from os.path import join, isfile

uniquely_identified_data_folder = "./uniquely_identified_data"

input_path = next((join(uniquely_identified_data_folder, f) for f in os.listdir(uniquely_identified_data_folder)
 if isfile(join(uniquely_identified_data_folder, f))), "default value here")

file_name = os.path.basename(input_path).split('/')[-1]

mapmatched_data_folder = "./mapmatched_data"

output_path = mapmatched_data_folder + '/' + file_name

archive_folder = "./archived_uniquely_identified_data"

df = pd.read_csv(input_path, encoding='iso-8859-1', sep=';')

shutil.copy(input_path, archive_folder)

os.remove(input_path)

request_data = {'shape': 'fill_here',
                'costing': 'bus',
                'shape_match': 'map_snap',
                'filters': {'attributes':['matched.point'],
                            'action': 'include'}
                }

unique_identifier_list = df['unique_identifier'].unique()[:200]
corrigated_df = pd.DataFrame()

df['time_stamp'] = pd.to_datetime(df['time_stamp'])

def mapmatching(unique_id = '105285--0'):
    tmp_df = df.loc[df['unique_identifier'] == unique_id]
    tmp_df['time_stamp'] = tmp_df['time_stamp'].apply(lambda x: (x-tmp_df['time_stamp'].iloc[0]).total_seconds()).astype(int)
    #1000 szögmásodpercben  megadva
    tmp_df = tmp_df[['GPS_V', 'GPS_H', 'time_stamp']]
    tmp_df['GPS_V'] = tmp_df['GPS_V'].apply(lambda x: x/3600000)
    tmp_df['GPS_H'] = tmp_df['GPS_H'].apply(lambda x: x/3600000)
    tmp_df.rename(columns={'GPS_V':'lat', 'GPS_H': 'lon', 'time_stamp': 'time'}, inplace=True)
    res = ast.literal_eval(tmp_df.to_json(orient='records'))
    request_data['shape'] = res
    r = requests.post(' http://valhalla:8002/trace_attributes', json=request_data)
    r_json = r.json()
    jsonString = json.dumps(r_json['matched_points'])
    #if jsonString == "{'error_code': 444, 'error': 'Map Match algorithm failed to find path:map_snap algorithm failed to snap the shape points to the correct shape.', 'status_code': 400, 'status': 'Bad Request'}":
    lat_lon_df = pd.read_json(jsonString)
    tmp_df['lat'] = lat_lon_df['lat'].values
    tmp_df['lon'] = lat_lon_df['lon'].values
    tmp_df.drop(columns=['time'], inplace=True)
    return pd.concat([df,tmp_df], axis=1, join='inner')

for i in unique_identifier_list:
    try:
        corrigated_df = corrigated_df.append(mapmatching(i))
    except:
        continue

def new_coordinates(df_corr):
    df_corr['GPS_H'] = df_corr['lat']
    df_corr['GPS_V'] = df_corr['lon']
    df_corr.drop(['lat', 'lon'], axis=1, inplace=True)
    df_corr.rename(columns={'GPS_H':'lat', 'GPS_V': 'lon'}, inplace=True)

new_coordinates(corrigated_df)

print('Mapmatching is DONE!')

corrigated_df.to_csv(output_path,index=False, encoding='iso-8859-1', sep=';')

#os.remove(input_path)
