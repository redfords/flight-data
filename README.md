# Flight data analysis

Real-time flight data from Aviationstack API. Using .json and .csv files, a single source is loaded into the MySQL database.

## Modules

- Requests
- Pandas
- Numpy
- SQLAlchemy
- Pytest
- Apache Airflow

## Data Model

Dimension Tables:
`country`
`city`
`airport`
`airline`

Fact Tables:
`flight`

![Model](https://i.imgur.com/5xvZeph.jpg)
