from ingest import db_config
from sqlalchemy.engine import create_engine
from sqlalchemy.sql import text


# Test the record counts in yields tables
def test_yield_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = "select count(*) from yielddata"
        db_count = con.execute(text(query_string)).scalar()
        assert db_count == 30


def test_yield_sample_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = "select * from yielddata where yield_year=1985"
        yield_data = con.execute(text(query_string)).fetchall()
        assert yield_data[0][0] == 1985
        assert yield_data[0][1] == 225447


# Test the record counts in  weather table
def test_weather_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = "select count(*) from weatherdata"
        db_count = con.execute(text(query_string)).scalar()
        assert db_count == 1729957


def test_weather_sample_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = "select * from weatherdata where weather_station='USC00110072' and precipitation is null and " \
                       "max_temp=172 "
        weather_data = con.execute(text(query_string)).fetchall()
        assert weather_data[0][0] == 'USC00110072'
        assert weather_data[0][1].strftime('%d-%m-%Y') == '05-05-1991'
        assert weather_data[0][2] == 172
        assert weather_data[0][3] == 72
        assert weather_data[0][4] == None


# Test the record counts in statistics table
def test_statistics_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = "select count(*) from statistics"
        db_count = con.execute(text(query_string)).scalar()
        assert db_count == 4820


def test_statistics_sample_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string1 = "select * from statistics where weather_station='USC00110072' and yield_year=1985"
        query_string2 = """select WEATHER_STATION,
            extract(year from created_date) yield_year, 
            avg(max_temp)  avg_max_temp,avg(min_temp)  avg_min_temp,
            sum(PRECIPITATION)  total_precipitation from weatherdata where weather_station='USC00110072' and extract(year from created_date)=1985 
            group by WEATHER_STATION,extract(year from created_date)"""
        target_data = con.execute(text(query_string1)).fetchall()
        source_data = con.execute(text(query_string2)).fetchall()
        assert target_data[0][0] == source_data[0][0]
        assert target_data[0][1] == source_data[0][1]
        assert float(target_data[0][2]) == round(float(source_data[0][2]) / 10, 2)
        assert float(target_data[0][3]) == round(float(source_data[0][3]) / 10, 2)
        assert float(target_data[0][4]) == round(float(source_data[0][4]) / 100, 2)


# Test if -9999 loaded as Null in database.
def test_null_weather_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    with engine.connect() as con:
        query_string = """select * from WEATHERDATA where weather_station='USC00110072' and max_temp is null
and precipitation=0 and extract(day from created_date)=23 and extract(year from created_date)=1991"""
        db_count = con.execute(text(query_string)).fetchall()
        assert db_count[0][2] is None
        assert db_count[0][3] is None