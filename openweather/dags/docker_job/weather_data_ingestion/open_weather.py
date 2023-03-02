import os
import time
from ast import literal_eval
from datetime import datetime, timedelta

import pandas as pd
import requests
import yaml
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient


def create_dag(
		dag_id,
		schedule,
		task_id,
		description,
		start_date,
		default_args,
		ingestion_function,
		tables,
):
	dag = DAG(
		dag_id=dag_id,
		start_date=start_date,
		description=description,
		default_args=default_args,
		schedule_interval=schedule,
		template_searchpath=['/opt/airflow/dags']
	)
	azure_connection = BaseHook.get_connection('azure_connection')
	with dag:
		for tbl in tables:
			ingest_weather_data = PythonOperator(
				task_id=f"{task_id}-{tbl}",
				provide_context=True,
				python_callable=ingestion_function,
				op_kwargs={
					"task_id": task_id,
					"directory": tbl,
					"location": tables[tbl],
					"azure_connection": azure_connection.get_extra()
				}
			)
			ingest_weather_data.doc_md = f"Ingests data for {tbl} table"
	
	return dag


def upload_to_azure_blob(filename, account_name=None, account_key=None, container=None, weather="weather"):
	connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey=" \
	                    f"{account_key};EndpointSuffix=core.windows.net"
	blob_service_client = BlobServiceClient.from_connection_string(connection_string)
	blob_client = blob_service_client.get_blob_client(container=container, blob=f"{weather}{filename[4:]}")
	
	with open(filename, 'rb') as data:
		blob_client.upload_blob(data)


def path_loc(root_dir, directory):
	data_stage = []
	timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-00')
	dir_with_ts = datetime.now().strftime('%Y-%m-%d')
	dir_path = os.path.join(root_dir, directory, dir_with_ts)
	os.makedirs(dir_path, exist_ok=True)
	filename = os.path.join(dir_path, f'{timestamp}_weather_data.parquet')
	data_stage.append(dir_path)
	
	return {
		"data_stage": data_stage,
		"filename": filename,
		"dir_with_ts": dir_with_ts,
		"timestamp": timestamp,
	}


def get_api_key(azure_connection, secret_names):
	# Replace <key_vault_url> with the URL of your Azure Key Vault
	key_vault_url = literal_eval(azure_connection)['Key_Vault_Url']

	# Create an instance of DefaultAzureCredential to authenticate with Azure
	credential = DefaultAzureCredential()
	
	# Create an instance of SecretClient to interact with the Key Vault
	secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
	
	# Replace <secret_name> with the name of the secret you want to retrieve
	secret_name = secret_names
	
	# Get the secret value
	secrets = secret_client.get_secret(secret_name).value
	
	return secrets


def get_weather_data(**kwargs):
	azure_connection = kwargs['azure_connection']
	api_key = get_api_key(
		azure_connection,
		literal_eval(azure_connection)['Secrets']['open-weather-api-key']
	)
	account_key = get_api_key(
		azure_connection,
		literal_eval(azure_connection)['Secrets']['AccountKey']
	)
	root_dir = "/tmp/"
	directory = [kwargs["directory"]]
	dfs, lat_long_pairs, data_stage = [], [], []
	lat_long_pairs.append((kwargs['location']['long'][0], kwargs['location']['lat'][0]))
	try:
		for lat_lon, dirr in zip(lat_long_pairs, directory):
			for i in range(60):
				url = f'https://api.openweathermap.org/data/2.5/weather?lat=' \
				      f'{lat_lon[0]}&lon={lat_lon[1]}&appid=' \
				      f'{api_key}&units=metric'
				dfs.append(pd.json_normalize(requests.get(url).json()))
			time.sleep(1)
			df = pd.concat(dfs, ignore_index=True)
			path_location = path_loc(root_dir, dirr)
			df.to_parquet(path_location["filename"])
			upload_to_azure_blob_task = PythonOperator(
				task_id='upload_to_azure_blob',
				python_callable=upload_to_azure_blob,
				op_kwargs={
					'filename': path_location["filename"],
					'account_name': literal_eval(azure_connection)['AccountName'],
					'account_key': account_key,
					'container': literal_eval(azure_connection)['Container']
				},
				dag=kwargs["dag"],
			)
			delete_local_data = BashOperator(
				task_id=f"start_clean-local-data-{path_location['filename'].split('_')[1].replace('/', '-')}",
				bash_command=f"rm -rf {path_location['filename'].replace('.csv', '.parquet')}",
				dag=kwargs["dag"],
			)
			upload_to_azure_blob_task.execute(dict())
			delete_local_data.execute(dict())
	except Exception as ex:
		print(ex)
	
	return data_stage


task_id = "get-openweather-data"

default_args = {
	'depends_on_past': False,
	'email': ['samuelolle@yahoo.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'catchup': False,
	'max_active_runs': 1,
	'retry_delay': timedelta(minutes=3),
	'execution_timeout': timedelta(hours=1),
}

start_date = datetime(2022, 8, 23)
with open("dags/docker_job/yaml_files/open_weather.yml") as file_loc:
	# with open("../yaml_files/open_weather.yml") as file_loc:
	source_table = yaml.load_all(file_loc, Loader=yaml.FullLoader)
	for tbl in source_table:
		for key, val in tbl.items():
			dag_id = f'Job-{key}-get-openweather-data'
			description = f'Retrieves weather data from OpenWeather API and stores it to Azure Blob Storage'
			globals()[dag_id] = create_dag(
				dag_id=dag_id,
				schedule=val['schedule'],
				task_id=task_id,
				description=description,
				start_date=start_date,
				default_args=default_args,
				ingestion_function=get_weather_data,
				tables=val['table_name'],
			)
