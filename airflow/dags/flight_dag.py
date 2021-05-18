from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

def load_data():
	mysql_hook = MySqlHook(mysql_conn_id = 'flight_id')

	table_name = ['country', 'city', 'airport', 'airline', 'flight']

	for table in table_name:
		# convert .csv into dataframe
		data = pd.read_csv('~/airflow/dags/files/' + table_name + '.csv')
		data = data.iloc[:, 1:]

		# convert dataframe into list of tuples
		rows = list(data.itertuples(index = False, name = None))

		# insert list of tuples into db
		mysql_hook.insert_rows(table = table_name, rows = rows)

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

# get the flight data from aviation stack
task1 = BashOperator(
	task_id = 'get_flight',
	bash_command = 'python ~/airflow/dags/get_flight_data.py' ,
	dag = dag
	)

# get the airport and airline data from aviation stack
task2 = BashOperator(
	task_id = 'get_airport',
	bash_command = 'python ~/airflow/dags/get_airport_data.py' ,
	dag = dag
	)

# process and load into the database
task3 =  PythonOperator(
	task_id = 'transform_load',
	provide_context = True,
	python_callable = load_data,
	dag = dag
	)

# task hierarchy
(task1, task2) >> task3