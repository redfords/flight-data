DROP TABLE if EXISTS flight;
DROP TABLE if EXISTS airport;
DROP TABLE if EXISTS airline;
DROP TABLE if EXISTS city;	
DROP TABLE if EXISTS country;

CREATE TABLE country (
	country_id INT,
	capital VARCHAR(20),
	currency_code CHAR(3),
	fips_code CHAR(2),
	country_iso2 CHAR(2),
    country_iso3 CHAR(3),
    continent CHAR(2),
	country_name VARCHAR(60),
    currency_name VARCHAR(15),
    country_iso_numeric INT,
	phone_prefix VARCHAR(20),
    population INT,
    PRIMARY KEY (country_iso2)
);

CREATE TABLE city (
	city_id INT,
	gmt INT,
	iata_code CHAR(3),
	country_iso2 CHAR(2),
	geoname_id INT,
	latitude VARCHAR(20),
	longitude VARCHAR(20),
	city_name VARCHAR(30),
	timezone VARCHAR(40),
	PRIMARY KEY (iata_code),
	FOREIGN KEY (country_iso2) REFERENCES country(country_iso2)
);

CREATE TABLE airport (
	airport_id INT,
	gmt INT,
	iata_code CHAR(3),
	city_iata_code CHAR(3),
	icao_code CHAR(4),
	country_iso2 CHAR(2),
	geoname_id INT,
	latitude VARCHAR(20),
	longitude VARCHAR(20),
	airport_name VARCHAR (100),
	country_name VARCHAR(60),
	phone_number VARCHAR(50),
	timezone VARCHAR(40),
	PRIMARY KEY (icao_code),
	FOREIGN KEY (city_iata_code) REFERENCES city(iata_code)
);

CREATE TABLE airline (
	airline_id INT,
	fleet_average_age FLOAT,
	callsign VARCHAR(30),
	hub_code CHAR(3),
	iata_code CHAR(2),
	icao_code CHAR(3),
	country_iso2 CHAR(2),
	date_founded INT,
	iata_prefix_accounting INT,
	airline_name VARCHAR(60),
	country_name VARCHAR(60),
	fleet_size INT,
	STATUS VARCHAR(7),
	TYPE VARCHAR(20),
	PRIMARY KEY (airline_name),
	FOREIGN KEY (country_iso2) REFERENCES country(country_iso2)
);

CREATE TABLE flight (
    flight_id INT,
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
    airline_iata CHAR(2),
    airline_icao CHAR(3),
    flight_number INT,
    flight_iata VARCHAR(7),
    flight_icao VARCHAR(8),
    PRIMARY KEY (flight_id),
	FOREIGN KEY (departure_icao) REFERENCES airport(icao_code),
    FOREIGN KEY (arrival_icao) REFERENCES airport(icao_code),
    FOREIGN KEY (airline_name) REFERENCES airline(airline_name)
)
