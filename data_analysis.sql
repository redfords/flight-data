-- Flights by airline and status (active, scheduled or cancelled)
SELECT airline_name, flight_status, COUNT(flight_id) AS flights
FROM flight
GROUP BY airline_name, flight_status
ORDER BY COUNT(flight_id) DESC, airline_name

-- Scheduled departures and arrivals by airport for today
SELECT airport.airport_name,
    COUNT(departure.flight_id) AS departures,
    COUNT(arrival.flight_id) AS arrivals
FROM airport
LEFT JOIN (
	SELECT *
	FROM flight
	WHERE date_format(departure_scheduled,'%Y-%m-%d') = date_format(NOW(),'%Y-%m-%d')) AS departure
ON departure.departure_icao = airport.icao_code
LEFT JOIN (
	SELECT *
	FROM flight
	WHERE date_format(arrival_scheduled,'%Y-%m-%d') = date_format(NOW(),'%Y-%m-%d')) AS arrival
ON arrival.arrival_icao = airport.icao_code
GROUP BY airport.airport_name
HAVING departures > 0 OR arrivals > 0
ORDER BY airport_name

-- Airlines with two or more flights scheduled for this month
SELECT airline_name, COUNT(flight_id) AS flights
FROM flight
WHERE DATE_FORMAT(flight_date,'%Y-%m') = date_format(NOW(),'%Y-%m')
GROUP BY airline_name
HAVING COUNT(flight_id) > 1
ORDER BY COUNT(flight_id) DESC, airline_name

-- Top five cities with most arrivals per day this month
SELECT ranking.day, ranking.city_name, ranking.total_arrivals
FROM (
	SELECT total.day, total.city_name, total.total_arrivals,
	ROW_NUMBER() over (PARTITION BY total.day ORDER BY total.total_arrivals DESC) AS row_num
	FROM (
	SELECT DISTINCT DATE_FORMAT(arrival_scheduled,'%Y-%m-%d') AS 'day', city_name,
		COUNT(flight_id) over (
			PARTITION BY DATE_FORMAT(arrival_scheduled,'%Y-%m-%d'), city_name) total_arrivals
		FROM flight
		INNER JOIN airport
		ON flight.arrival_icao = airport.icao_code
		INNER JOIN city
		ON airport.city_iata_code = city.iata_code) total) ranking
WHERE row_num <= 5

-- Airlines with no flights scheduled for today
SELECT airline.airline_name
FROM airline
LEFT JOIN (
	SELECT *
	FROM flight
	WHERE flight_date = date_format(NOW(),'%Y-%m-%d')) AS flight_today
ON airline.icao_code = flight_today.airline_icao
WHERE flight_today.airline_icao IS NULL
ORDER BY airline.airline_name

-- Active airlines founded before 1950
SELECT airline_name, date_founded
FROM airline
WHERE date_founded <= 1950 AND
    status = 'active'
ORDER BY date_founded ASC

-- Top ten airlines with most cancelled flights per month this year
SELECT ranking.month, ranking.airline_name, ranking.total_flights
FROM (
	SELECT total.month, total.airline_name, total.total_flights,
	ROW_NUMBER() over (PARTITION BY total.month ORDER BY total.total_flights DESC) AS row_num
	FROM (
		SELECT DISTINCT DATE_FORMAT(flight_date,'%Y-%m') as 'month', airline_name,
			COUNT(flight_id) over (
                PARTITION BY DATE_FORMAT(flight_date,'%Y-%m'), airline_name) total_flights
		FROM flight
		WHERE flight_status = 'cancelled') total ) ranking
WHERE row_num <= 10

-- Create historical flights table and insert today's records
CREATE TABLE historical_flight
AS
SELECT *
FROM flight
WHERE 1 = 0

*/
DELIMITER //
    CREATE PROCEDURE historical_flight()
    BEGIN
        INSERT INTO historical_flight (
            flight_id,
            flight_date,
            flight_status,
            departure_airport,
            departure_timezone,
            departure_iata,
            departure_icao,
            departure_terminal,
            departure_gate,
            departure_delay,
            departure_scheduled,
            departure_estimated,
            departure_actual,
            departure_estimated_runway,
            departure_actual_runway,
            arrival_airport,
            arrival_timezone,
            arrival_iata,
            arrival_icao,
            arrival_terminal,
            arrival_gate,
            arrival_baggage,
            arrival_delay,
            arrival_scheduled,
            arrival_estimated,
            arrival_actual,
            arrival_estimated_runway,
            arrival_actual_runway,
            airline_name, 
            airline_iata,
            airline_icao,
            flight_number,
            flight_iata,
            flight_icao )
        SELECT *
        FROM flight
        WHERE flight_date = date_format(NOW(),'%Y-%m-%d')); 
    END;
   //
DELIMITER

--US airline categories by fleet size: large hub 400 or more aircrafts
--medium hub between 100 and 400 aircrafts, small hub less than 100 aircrafts
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

-- Total flights from US by month this year
SELECT date_format(departure_scheduled,'%Y-%m') AS MONTH,
	COUNT(flight_id) AS departures
FROM flight
INNER JOIN airport
ON airport.icao_code = flight.departure_icao
WHERE date_format(departure_scheduled,'%Y') = date_format(NOW(),'%Y') AND
    country_iso2 = 'US'
GROUP BY date_format(departure_scheduled,'%Y-%m')

