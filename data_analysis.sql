-- Scheduled departures and arrivals by airport for today
SELECT airport, COUNT(*)
FROM flight


-- Flights by airline and status (active, scheduled or cancelled)
SELECT airline_name, flight_status, COUNT(flight_id) AS flights
FROM flight
GROUP BY airline_name, flight_status
ORDER BY COUNT(flight_id) DESC, airline_name

-- Airlines with two or more flights scheduled for this month
SELECT airline_name, COUNT(flight_id) AS flights
FROM flight
WHERE DATE_FORMAT(flight_date,'%Y-%m') = date_format(NOW(),'%Y-%m')
GROUP BY airline_name
HAVING COUNT(flight_id) > 1
ORDER BY COUNT(flight_id) DESC, airline_name

-- Top five cities with most flights per day this month


-- Top ten airlines with most cancelled flights per month this year


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
WHERE date_founded <= 1950
ORDER BY date_founded ASC

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

-- Total arrivals and departures from US by month this year


/*Por día se necesita, cantidad de ventas realizadas, cantidad de productos vendidos y monto total
transaccionado para el mes de Enero del 2020.
*/
SELECT fecha, COUNT(distinct orden.orden_id) AS cant_ventas, SUM(cantidad) AS cant_productos,
    SUM(cantidad * precio) AS monto_total
FROM orden
JOIN orden_item
ON orden.orden_id = orden_item.orden_id
JOIN item
ON item.item_id = orden_item.item_id
WHERE DATE_FORMAT(fecha,'%Y-%m') = '2020-01'
GROUP BY DATE_FORMAT(fecha,'%d'), fecha

/*Por cada mes del 2019, se solicita el top 5 de usuarios que más vendieron ($) en la categoría
Celulares. Se requiere el mes y año de análisis, nombre y apellido del vendedor, la cantidad vendida
y el monto total transaccionado.
*/
SELECT m.mes, m.nombre, m.apellido, cant_vendida, monto_total 
FROM (
	SELECT temp.mes, temp.nombre, temp.apellido, temp.cant_vendida, temp.monto_total,
	    ROW_NUMBER() over (PARTITION BY temp.mes ORDER BY temp.monto_total DESC) AS row_num
	FROM (
		SELECT distinct DATE_FORMAT(fecha,'%Y-%m') mes, nombre, apellido,
		    SUM(cantidad) over (PARTITION BY DATE_FORMAT(fecha,'%Y-%m'), seller_id) cant_vendida,
		    SUM(cantidad * precio) over (PARTITION BY DATE_FORMAT(fecha,'%Y-%m'), seller_id) monto_total
		FROM customer
		JOIN item
		ON customer.customer_id = item.seller_id
		JOIN orden_item
		ON item.item_id = orden_item.item_id
		JOIN orden
		ON orden_item.orden_id = orden.orden_id) temp ) m
WHERE row_num <= 5

/*Se solicita poblar una tabla con el precio y estado de los Items a fin del día (se puede resolver
a través de StoredProcedure). Vale resaltar que en la tabla Item, vamos a tener únicamente el último
estado informado por la PK definida. Esta información nos va a permitir realizar análisis para
entender el comportamiento de los diferentes Items (por ejemplo evolución de Precios, cantidad de
Items activos).
*/
DELIMITER //
    CREATE PROCEDURE item_historial()
    BEGIN
        INSERT INTO item_historial
            (DATE_FORMAT(NOW(),'%Y-%m-%d'),
            item_id,
            descripcion,
            estado,
            fecha_alta,
            fecha_baja,
            precio,
            category_id,
            seller_id)
        SELECT * FROM item; 
    END;
   //
DELIMITER

/*Desde IT nos comentan que la tabla de Categorías tiene un issue ya que cuando generan modificaciones
de una categoría se genera un nuevo registro con la misma PK en vez de actualizar el ya existente.
Teniendo en cuenta que tenemos una columna de Fecha de LastUpdated, se solicita crear una nueva tabla
y poblar la misma sin ningún tipo de duplicados garantizando la calidad y consistencia de los datos.
*/
CREATE TABLE category_2
AS
SELECT *
FROM category
WHERE 1 = 0

INSERT INTO category_2
SELECT c.category_id, c.descripcion, c.path, c.last_updated
FROM category c
INNER JOIN (
	SELECT descripcion, MAX(last_updated) AS max_date
	FROM category
	GROUP BY descripcion) c2
ON c.descripcion = c2.descripcion AND c.last_updated = c2.max_date

/*Suponiendo que sumes el monto de las ventas en las tabla de Orders para el periodo 2020-10 da como
resultado $ 100.000 pesos. Pero cuando se relaciona con la tabla de categorías el monto baja a $50.000
evidentemente hay códigos de categorías que se pierden ¿cómo podemos determinar cuales y qué monto se
pierden por cada una?
*/
SELECT item.category_id, SUM(cantidad * precio) AS monto_categoria
FROM orden
JOIN orden_item
ON orden.orden_id = orden_item.orden_id
JOIN item
ON item.item_id = orden_item.item_id
LEFT JOIN category
ON category.category_id = item.category_id
WHERE category.category_id IS NULL 
GROUP BY item.category_id
