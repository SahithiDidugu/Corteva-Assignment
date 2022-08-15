# Corteva-Assignment
Assignment

Problem 1 - Data Modeling
-------------------------
#Choose a database to use for this coding exercise (SQLite, Postgres, etc.). Design two data models: one to represent the weather data records, and one to represent the yield data records. If you use an ORM, your answer should be in the form of that ORM's data definition format. If you use pure SQL, your answer should be in the form of DDL statements.
Database- Oracle.
created 2 models WeatherData and YieldData in Django models.py, but assigned 'managed=False', as Django is not supporting composite Primary key
so created DDL statements in DDL.txt file at "\corteva_agri\DDL\DDL.txt"

Problem 2 - Ingestion
---------------------
Write code to ingest the weather and yield data from the raw text files supplied into your database, using the models you designed. Check for duplicates: if your code is run twice, you should not end up with multiple rows with the same data in your database. Your code should also produce log output indicating start and end times and number of records ingested.
----------------------------------------------------------------------------------
For Ingestion, created a ingest.Py ("\corteva_agri\ingest.py").
also placed the source files at same location(\corteva_agri\wx_data) and (\corteva_agri\yld_data).
In ingest.py , i created 3 functions load_yield_data(),load_weather_data(),load_statistics()
i used pandas to read the csv files , also used glob.glob to iterate multiple files in the folder(for wx_data)
using sqlalchemy.engine , to load csv files to oracle DB.

Problem 3 - Data Analysis
-------------------------
For every year, for every weather station, calculate:

* Average maximum temperature (in degrees Celsius)
* Average minimum temperature (in degrees Celsius)
* Total accumulated precipitation (in centimeters)

Ignore missing data when calculating these statistics.

Design a new data model to store the results. Use NULL for statistics that cannot be calculated.

Your answer should include the new model definition as well as the code used to calculate the new values and store them in the database.
---------------------------------------------------------------------------
created a new model statistics and inserted calculated the above measures in ingest.py(load_statistics())
Also, added log file , ingest.log to view the start and end times of data loading details.
Problem 4 - REST API
--------------------
Choose a web framework (e.g. Flask, Django REST Framework). Create a REST API with the following GET endpoints:

/api/weather
/api/yield
/api/weather/stats

Each endpoint should return a JSON-formatted response with a representation of the ingested data in your database. Allow clients to filter the response by date and station ID (where present) using the query string. Data should be paginated.

Your answer should include all files necessary to run your API locally, along with any unit tests.

----------------------------------------------------------
used django-restframework , django-filters to build the REST API with model serializers in views.py
Added pagination details in settings.py,
Added query string parameters for date and weather_station.

Submitting Your Answers
-----------------------
Consider using a linter, code formatter, and including tests and code comments. Anything that helps us understand your thought process is helpful!

Please provide us with a link to your Git repository, hosted on GitHub/GitLab, containing your answers and code.
