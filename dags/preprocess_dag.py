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
'retries'               : 0,
'retry_delay'           : timedelta(minutes=1)
}


@dag(default_args=default_args, schedule_interval="@daily", catchup=False, max_active_runs=5, concurrency=5)
def preprocess_dag():
   
   
   preprocess = DockerOperator(
       task_id="preprocess",
       max_active_tis_per_dag=5,
       docker_url="unix://var/run/docker.sock",
       mount_tmp_dir=False,
       image="mgregosits/preprocess:0.0.4",
       command="python preprocess.py",
       network_mode="valhalla_default",
       mounts=[
    	Mount("/input", "/home/gmate/input", type='bind'),
	]
   )


    
   


dag = preprocess_dag()