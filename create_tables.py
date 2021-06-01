from sqlalchemy import create_engine

# connect to database
engine = create_engine(
    "mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(
        user = "root",
        pw = "password",
        host = "localhost:33060",
        db = "test"
    )
)               

# drop table if exists
def drop_table(tables):
	for table in tables:
		query = f"DROP TABLE if EXISTS {table};"
		engine.execute(query)

tables = ['flights', 'airplane', 'airport', 'airline', 'city', 'country', 'date']

# drop_table(tables)

# table columns and data types
# foreign keys missing due to API free-tier request limit
date = """
	date_id INT NOT NULL AUTO_INCREMENT,
	date DATE,
	year INT,
	month INT,
	month_name TEXT,
	day INT,
	weekday_name TEXT,
	calendar_week INT,
	quarter TEXT,
	PRIMARY KEY (date_id)
"""

country = """
	country_id INT NOT NULL AUTO_INCREMENT,
	capital VARCHAR(20),
	currency_code CHAR(3),
	fips_code CHAR(2),
	country_iso2 CHAR(2) NOT NULL,
	country_iso3 CHAR(3),
	continent CHAR(2),
	country_name VARCHAR(60),
	currency_name VARCHAR(15),
	country_iso_numeric INT,
	phone_prefix VARCHAR(20),
	population INT,
	PRIMARY KEY (country_id)
"""

city = """
	city_id INT NOT NULL AUTO_INCREMENT,
	gmt INT,
	iata_code CHAR(3) NOT NULL,
	country_iso2 CHAR(2),
	geoname_id INT,
	latitude VARCHAR(20),
	longitude VARCHAR(20),
	city_name VARCHAR(30),
	timezone VARCHAR(40),
	PRIMARY KEY (city_id)
"""

airport = """
	airport_id INT NOT NULL AUTO_INCREMENT,
	gmt INT,
	iata_code CHAR(3),
	city_iata_code CHAR(3),
	icao_code CHAR(4) NOT NULL,
	country_iso2 CHAR(2),
	geoname_id VARCHAR(8),
	latitude VARCHAR(20),
	longitude VARCHAR(20),
	airport_name VARCHAR (100),
	country_name VARCHAR(60),
	phone_number VARCHAR(50),
	timezone VARCHAR(40),
	PRIMARY KEY (airport_id)
"""

airline = """
	airline_id INT NOT NULL AUTO_INCREMENT,
	fleet_average_age FLOAT,
	callsign VARCHAR(30),
	hub_code CHAR(3),
	iata_code CHAR(2),
	icao_code CHAR(3),
	country_iso2 CHAR(2),
	date_founded YEAR,
	iata_prefix_accounting INT,
	airline_name VARCHAR(60) NOT NULL,
	country_name VARCHAR(60),
	fleet_size INT,
	STATUS VARCHAR(7),
	TYPE VARCHAR(20),
	PRIMARY KEY (airline_id)
"""

airplane = """
	airplane_id INT NOT NULL AUTO_INCREMENT,
	registration_number VARCHAR(10),
	production_line VARCHAR(80),
	iata_type VARCHAR(10),
	model_name VARCHAR(10),
	model_code VARCHAR(10),
	icao_code_hex VARCHAR(10),
	iata_code_short VARCHAR(10),
	PRIMARY KEY (airplane_id)
"""

flights = """
	flight_id INT NOT NULL AUTO_INCREMENT,
	flight_date DATE,
	flight_status VARCHAR(10),
	departure_icao CHAR(4),
	departure_terminal CHAR(5),
	departure_gate CHAR(5),
	departure_delay INT,
	departure_scheduled DATETIME,
	departure_estimated DATETIME,
	departure_actual DATETIME,
	departure_estimated_runway DATETIME,
	departure_actual_runway DATETIME,
	arrival_icao CHAR(4),
	arrival_terminal CHAR(5),
	arrival_gate CHAR(5),
	arrival_delay INT,
	arrival_scheduled DATETIME,
	arrival_estimated DATETIME,
	arrival_actual DATETIME,
	arrival_estimated_runway DATETIME,
	arrival_actual_runway DATETIME,
	airline_icao CHAR(3),
	flight_number VARCHAR(5),
	flight_iata VARCHAR(7),
	flight_icao VARCHAR(8),
	PRIMARY KEY (flight_id)
"""

# create tables in database
def create_table(tables):
    for key, value in tables.items():
        engine.execute(f'CREATE TABLE IF NOT EXISTS {value} (' + key + ');')

tables = {
	date: 'date',
    country: 'country',
    city: 'city',
    airport: 'airport',
    airline: 'airline',
    flights: 'flights'
}

create_table(tables)

# populate date dimension table
date_dim = """
    CREATE TABLE date_temp (
        datum DATE NOT NULL
    )

    INSERT INTO date_temp
    WITH RECURSIVE date_range AS (
        SELECT '2020-01-01' as datum
        UNION ALL
        SELECT datum + interval 1 day
        FROM date_range
        WHERE datum < '2021-12-31')
    SELECT datum
    FROM date_range;

    INSERT INTO date(date, year, month, month_name, day, weekday_name, calendar_week, quarter)
    SELECT
        datum as date,
        EXTRACT(YEAR FROM datum) as year,
        EXTRACT(MONTH FROM datum) as month,
        MONTHNAME(datum) as month_name,
        EXTRACT(DAY FROM datum) as day,
        DAYNAME(datum) as weekday_name,
        EXTRACT(week FROM datum) as calendar_week,
        QUARTER(datum) as quarter
    FROM date_temp;

    DROP TABLE date_temp;
"""

engine.execute(date_dim)