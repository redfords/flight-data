from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import data_analysis

mysql_hook = MySqlHook(mysql_conn_id = 'flight_id')

def load_data():
	# define target fields
	country = [
		'capital',
		'currency_code',
		'fips_code',
		'country_iso2',
		'country_iso3',
		'continent',
		'country_name',
		'currency_name',
		'country_iso_numeric',
		'phone_prefix',
		'population'
	]

	city = [
		'gmt',
		'iata_code',
		'country_iso2',
		'geoname_id',
		'latitude',
		'longitude',
		'city_name',
		'timezone'
	]

	airport = [
		'gmt',
		'iata_code',
		'city_iata_code',
		'icao_code',
		'country_iso2',
		'geoname_id',
		'latitude',
		'longitude',
		'airport_name',
		'country_name',
		'phone_number',
		'timezone'
	]

	airline = [
		'fleet_average_age',
		'callsign',
		'hub_code',
		'iata_code',
		'icao_code',
		'country_iso2',
		'date_founded',
		'airline_name',
		'fleet_size',
		'STATUS',
		'TYPE'
	]

	airplane = [
		'iata_type',
		'iata_code_short',
		'construction_number',
		'delivery_date',
		'engines_count',
		'engines_type',
		'first_flight_date',
		'icao_code_hex'
	]

	flights = [
		'flight_date',
		'flight_status',
		'departure_icao',
		'departure_terminal',
		'departure_gate',
		'departure_delay',
		'departure_scheduled',
		'departure_estimated',
		'departure_actual',
		'departure_estimated_runway',
		'departure_actual_runway',
		'arrival_icao',
		'arrival_terminal',
		'arrival_gate',
		'arrival_delay',
		'arrival_scheduled',
		'arrival_estimated',
		'arrival_actual',
		'arrival_estimated_runway',
		'arrival_actual_runway',
		'airline_icao',
		'flight_number',
		'flight_iata',
		'flight_icao'
]

	# define tables to load
	table_name = {
		'country': country,
		'city': city,
		'airport': airport,
		'airline': airline,
		'airplane': airplane,
		'flights': flights
		}

	for table, target_fields in table_name.items():
		# convert .csv into dataframe
		data = pd.read_csv('~/airflow/dags/files/' + table + '.csv')
		data = data.iloc[:, 1:]
		replace_na = data.fillna(0)

		# convert dataframe into list of tuples
		rows = list(replace_na.itertuples(index = False, name = None))

		# insert list of tuples into db
		mysql_hook.insert_rows(
			table = table,
			rows = rows,
			target_fields = target_fields
		)

def run_data_analysis():
	queries = data_analysis.sql_queries()

	with pd.ExcelWriter('~/airflow/dags/files/output.xlsx') as writer:
		for key, value in queries.items():
			data = mysql_hook.get_pandas_df(key)
			data.to_excel(writer, index = False, sheet_name = value)

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
	dag_id = 'flight_dag',
	default_args = default_args,
	start_date = datetime(2021,5,17),
	schedule_interval = timedelta(minutes = 1440)
	)

# get the flight data from aviation stack
task1 = BashOperator(
	task_id = 'get_flight_data',
	bash_command = 'python ~/airflow/dags/get_flight_data.py' ,
	dag = dag
	)

# get the airport data from aviation stack
task2 = BashOperator(
	task_id = 'get_airport_data',
	bash_command = 'python ~/airflow/dags/get_airport_data.py' ,
	dag = dag
	)

# get the airline data from aviation stack
task3 = BashOperator(
	task_id = 'get_airline_data',
	bash_command = 'python ~/airflow/dags/get_airline_data.py' ,
	dag = dag
	)

# get the airplane data from aviation stack
task4 = BashOperator(
	task_id = 'get_airplane_data',
	bash_command = 'python ~/airflow/dags/get_airplane_data.py' ,
	dag = dag
	)

# process and load into the database
task5 =  PythonOperator(
	task_id = 'load_into_db',
	provide_context = True,
	python_callable = load_data,
	dag = dag
	)

# perform data analysis
task6 =  PythonOperator(
	task_id = 'run_data_analysis',
	provide_context = True,
	python_callable = run_data_analysis,
	dag = dag
	)

# task hierarchy
(task1, task2, task3, task4) >> task5 >> task6