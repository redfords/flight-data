from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def load_data():
	mysql_hook = MySqlHook(mysql_conn_id = 'flight_id')

	table_name = 'flight'

	# convert .csv into dataframe
	data = pd.read_csv('~/airflow/dags/files/' + table_name + '.csv')
	data = data.iloc[:, 1:]

	# convert dataframe into list of tuples
	rows = list(data.itertuples(index = False, name = None))

	# insert list of tuples into db
	mysql_hook.insert_rows(
		table = table_name,
		rows = rows,
		target_fields = [
			'flight_date',
			'flight_status',
			'departure_airport',
			'departure_timezone',
			'departure_iata',
			'departure_icao',
			'departure_terminal',
			'departure_gate',
			'departure_delay',
			'departure_scheduled',
			'departure_estimated',
			'departure_actual',
			'departure_estimated_runway',
			'departure_actual_runway',
			'arrival_airport',
			'arrival_timezone',
			'arrival_iata',
			'arrival_icao',
			'arrival_terminal',
			'arrival_gate',
			'arrival_baggage',
			'arrival_delay',
			'arrival_scheduled',
			'arrival_estimated',
			'arrival_actual',
			'arrival_estimated_runway',
			'arrival_actual_runway',
			'airline_name', 
			'airline_iata',
			'airline_icao',
			'flight_number',
			'flight_iata',
			'flight_icao'
            ]
		)

# define the default dag arguments
default_args = {
	'owner': 'joana',
	'depends_on_past': False,
	'email': ['joanapiovaroli@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 5,
	'retry_delay': timedelta(minutes = 1)
	}

# define the dag, start date and frequency
dag = DAG(
	dag_id = 'flight_dat',
	default_args = default_args,
	start_date = datetime(2021,5,17),
	schedule_interval = timedelta(minutes = 1440)
	)

# first task is to get the flight data from aviation stack
task1 = BashOperator(
	task_id = 'get_flight',
	bash_command = 'python ~/airflow/dags/get_flight_data.py' ,
	dag = dag
	)

# second task is to process the data and load into the database
task2 =  PythonOperator(
	task_id = 'transform_load',
	provide_context = True,
	python_callable = load_data,
	dag = dag
	)

# task1 must be completed before task2 can start
task1 >> task2