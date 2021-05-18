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

tables = ['flight', 'airport', 'airline', 'city', 'country']

drop_table(tables)

# table columns and data types
country = """
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
	PRIMARY KEY (country_iso2)
"""

city = """
	gmt INT,
	iata_code CHAR(3) NOT NULL,
	country_iso2 CHAR(2),
	geoname_id INT,
	latitude VARCHAR(20),
	longitude VARCHAR(20),
	city_name VARCHAR(30),
	timezone VARCHAR(40),
	PRIMARY KEY (iata_code)
"""

airport = """
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
	PRIMARY KEY (icao_code)
"""

airline = """
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
	PRIMARY KEY (airline_name)
"""

flight = """
	flight_id INT NOT NULL AUTO_INCREMENT,
	flight_date DATE,
	flight_status VARCHAR(10),
	departure_airport VARCHAR(100),
	departure_timezone VARCHAR(40),
	departure_iata CHAR(3),
	departure_icao CHAR(4),
	departure_terminal CHAR(5),
	departure_gate CHAR(5),
	departure_delay INT,
	departure_scheduled DATETIME,
	departure_estimated DATETIME,
	departure_actual DATETIME,
	departure_estimated_runway DATETIME,
	departure_actual_runway DATETIME,
	arrival_airport VARCHAR(100),
	arrival_timezone VARCHAR(40),
	arrival_iata CHAR(3),
	arrival_icao CHAR(4),
	arrival_terminal CHAR(5),
	arrival_gate CHAR(5),
	arrival_baggage CHAR(5),
	arrival_delay INT,
	arrival_scheduled DATETIME,
	arrival_estimated DATETIME,
	arrival_actual DATETIME,
	arrival_estimated_runway DATETIME,
	arrival_actual_runway DATETIME,
	airline_name VARCHAR(60), 
	airline_iata CHAR(3),
	airline_icao CHAR(3),
	flight_number VARCHAR(5),
	flight_iata VARCHAR(7),
	flight_icao VARCHAR(8),
	PRIMARY KEY (flight_id)
"""

# create tables in database
def create_table(tables):
    for key, value in tables.items():
        engine.execute(f'CREATE TABLE {value} (' + key + ');')

tables = {
    country: 'country',
    city: 'city',
    airport: 'airport',
    airline: 'airline',
    flight: 'flight'
}

create_table(tables)