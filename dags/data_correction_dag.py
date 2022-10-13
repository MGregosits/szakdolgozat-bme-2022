from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from docker.types import Mount
import docker

default_args = {
'owner'                 : 'Mate Gregosits',
'description'           : 'Correction of raw public transport data.',
'depend_on_past'        : False,
'start_date'            : datetime(2022, 9, 28),
'email_on_failure'      : False,
'email_on_retry'        : False,
'retries'               : 1,
'retry_delay'           : timedelta(minutes=5)
}
   
   


@dag(default_args=default_args, schedule_interval="@daily", catchup=False, max_active_runs=5, concurrency=5)
def data_correction_dag():


   bash = BashOperator(
      task_id="ls",
      bash_command="docker ps"
   )
   
   
   preprocess = DockerOperator(
       task_id="preprocess",
       max_active_tis_per_dag=5,
       docker_url="unix://var/run/docker.sock",
       image="mgregosits/preprocess:0.0.1",
       command="python preprocess.py",
       network_mode="valhalla_default",
       mounts=[
    	Mount("/raw_data", "/home/gmate/szakd/raw_data", type='bind'),
    	Mount("/preprocessed_data", "/home/gmate/szakd/preprocessed_data", type='bind'),
      Mount("/archived_raw_data", "/home/gmate/szakd/archived_raw_data", type='bind'),
      
	]
   )

   strip = DockerOperator(
       task_id="strip",
       max_active_tis_per_dag=5,
       docker_url="unix://var/run/docker.sock",
       image="mgregosits/strip:0.0.1",
       command="python strip.py",
       network_mode="valhalla_default",
       mounts=[
    	Mount("/stripped_data", "/home/gmate/szakd/stripped_data", type='bind'),
    	Mount("/preprocessed_data", "/home/gmate/szakd/preprocessed_data", type='bind'),
      Mount("/archived_preprocessed_data", "/home/gmate/szakd/archived_preprocessed_data", type='bind'),
      
	]
   )

   unique_identifier = DockerOperator(
       task_id="unique_identifier",
       max_active_tis_per_dag=5,
       docker_url="unix://var/run/docker.sock",
       image="mgregosits/unique_identifier:0.0.1",
       command="python unique_identifier.py",
       network_mode="valhalla_default",
       mounts=[
    	Mount("/stripped_data", "/home/gmate/szakd/stripped_data", type='bind'),
    	Mount("/uniquely_identified_data", "/home/gmate/szakd/uniquely_identified_data", type='bind'),
      Mount("/archived_stripped_data", "/home/gmate/szakd/archived_stripped_data", type='bind'),
      
	]
   )
   
   mapmatching = DockerOperator(
       task_id="mapmatching",
       max_active_tis_per_dag=5,
       docker_url="unix://var/run/docker.sock",
       image="mgregosits/mapmatching:0.0.1",
       command="python mapmatching.py",
       network_mode="valhalla_default",
       mounts=[
    	Mount("/mapmatched_data", "/home/gmate/szakd/mapmatched_data", type='bind'),
    	Mount("/uniquely_identified_data", "/home/gmate/szakd/uniquely_identified_data", type='bind'),
      Mount("/archived_uniquely_identified_data", "/home/gmate/szakd/archived_uniquely_identified_data", type='bind'),
      
	]
   )
    
   bash >> preprocess >> strip >> unique_identifier >> mapmatching


dag = data_correction_dag()







