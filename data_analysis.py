import pandas as pd
from openpyxl import Workbook
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

def sql_queries():
	# queries to execute
	# flights by airline and status (active, scheduled or cancelled)
	flight_status = """
	SELECT airline_name, flight_status, COUNT(flight_id) AS flights
	FROM flights
	GROUP BY airline_name, flight_status
	ORDER BY COUNT(flight_id) DESC, airline_name
	"""

	# scheduled departures and arrivals by airport for today
	arrival_today = """
	SELECT airport.airport_name,
		COUNT(departure.flight_id) AS departures,
		COUNT(arrival.flight_id) AS arrivals
	FROM airport
	LEFT JOIN (
		SELECT *
		FROM flights
		WHERE date_format(departure_scheduled,'%%Y-%%m-%%d') = date_format(NOW(),'%%Y-%%m-%%d')
		) AS departure
	ON departure.departure_icao = airport.icao_code
	LEFT JOIN (
		SELECT *
		FROM flights
		WHERE date_format(arrival_scheduled,'%%Y-%%m-%%d') = date_format(NOW(),'%%Y-%%m-%%d')
		) AS arrival
	ON arrival.arrival_icao = airport.icao_code
	GROUP BY airport.airport_name
	HAVING departures > 0 OR arrivals > 0
	ORDER BY airport_name
	"""

	# airlines with two or more flights scheduled for this month
	multiple_flights = """
	SELECT airline_name, COUNT(flight_id) AS number_of_flights
	FROM flights
	WHERE DATE_FORMAT(flight_date,'%%Y-%%m') = date_format(NOW(),'%%Y-%%m')
	GROUP BY airline_name
	HAVING COUNT(flight_id) > 1
	ORDER BY COUNT(flight_id) DESC, airline_name
	"""

	# top five cities with most arrivals per day this month
	cities_most_arrivals = """
	-- Top five cities with most arrivals per day this month
	SELECT ranking.day, ranking.city_name, ranking.total_arrivals
	FROM (
		SELECT total.day, total.city_name, total.total_arrivals,
		ROW_NUMBER() over (PARTITION BY total.day ORDER BY total.total_arrivals DESC) AS row_num
		FROM (
		SELECT DISTINCT DATE_FORMAT(arrival_scheduled,'%%Y-%%m-%%d') AS 'day', city_name,
			COUNT(flight_id) over (
				PARTITION BY DATE_FORMAT(arrival_scheduled,'%%Y-%%m-%%d'), city_name) total_arrivals
			FROM flights
			INNER JOIN airport
			ON flights.arrival_icao = airport.icao_code
			INNER JOIN city
			ON airport.city_iata_code = city.iata_code) total) ranking
	WHERE row_num <= 5
	"""

	# airlines with no flights scheduled for today
	no_flights = """
	SELECT airline.airline_name
	FROM airline
	LEFT JOIN (
		SELECT *
		FROM flights
		WHERE flight_date = date_format(NOW(),'%%Y-%%m-%%d')) AS flight_today
	ON airline.icao_code = flight_today.airline_icao
	WHERE flight_today.airline_icao IS NULL
	ORDER BY airline.airline_name
	"""

	# active airlines founded before 1950
	airline_founded = """
	SELECT airline_name, date_founded
	FROM airline
	WHERE date_founded <= 1950 AND
		status = 'active'
	ORDER BY date_founded ASC
	"""

	# top ten airlines with most cancelled flights per month this year
	most_cancelled_flights = """
	SELECT ranking.month, ranking.airline_name, ranking.total_flights
	FROM (
		SELECT total.month, total.airline_name, total.total_flights,
		ROW_NUMBER() over (PARTITION BY total.month ORDER BY total.total_flights DESC) AS row_num
		FROM (
			SELECT DISTINCT DATE_FORMAT(flight_date,'%%Y-%%m') as 'month', airline_name,
				COUNT(flight_id) over (
					PARTITION BY DATE_FORMAT(flight_date,'%%Y-%%m'), airline_name) total_flights
			FROM flights
			WHERE flight_status = 'cancelled') total ) ranking
	WHERE row_num <= 10
	"""

	# US airline categories by fleet size: large hub 400 or more aircrafts
	# medium hub between 100 and 400 aircrafts, small hub less than 100 aircrafts
	airline_category = """
	SELECT airline_name, fleet_size,
	CASE
		WHEN fleet_size > 400 THEN 'Large Hub'
		WHEN fleet_size > 100 THEN 'Medium Hub'
		ELSE 'Small Hub'
	END AS category
	FROM airline
	WHERE country_iso2 = 'US' AND
	STATUS = 'active'
	ORDER BY fleet_size DESC
	"""

	# total flights from US by month this year
	total_us_flights = """
	SELECT date_format(departure_scheduled,'%%Y-%%m') AS MONTH,
		COUNT(flight_id) AS departures
	FROM flights
	INNER JOIN airport
	ON airport.icao_code = flights.departure_icao
	WHERE date_format(departure_scheduled,'%%Y') = date_format(NOW(),'%%Y') AND
		country_iso2 = 'US'
	GROUP BY date_format(departure_scheduled,'%%Y-%%m')
	"""

	queries = {
		flight_status: 'flight_status',
		arrival_today: 'arrival_today',
		multiple_flights: 'multiple_flights',
		cities_most_arrivals: 'cities_most_arrivals',
		no_flights: 'no_flights',
		airline_founded: 'airline_founded',
		most_cancelled_flights: 'most_cancelled_flights',
		airline_category: 'airline_category',
		total_us_flights: 'total_us_flights'
	}

	return queries

# export into .xlsx file
def export_into_xlsx(queries):
    with pd.ExcelWriter('files/output.xlsx') as writer:
        for key, value in queries.items():
            data = pd.read_sql_query(key, engine)
            data.to_excel(writer, index = False, sheet_name = value)

export_into_xlsx(sql_queries())