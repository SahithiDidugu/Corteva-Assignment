import pandas as pd
import os
import glob
from pathlib import Path
import sqlalchemy
from sqlalchemy.engine import create_engine
import time
import logging
import cx_Oracle
from sqlalchemy.sql import text

logging.basicConfig(filename="ingest.log", level=logging.INFO)
BASE_DIR = Path(__file__).resolve().parent


def db_config():
    driver = 'cx_oracle'
    host = 'localhost'
    port = 1521
    service_name = 'orcl'
    schema = os.environ['dbuser']
    user = os.environ['dbuser']
    password = os.environ['dbpass']
    dns = cx_Oracle.makedsn(host, port, service_name)
    connstr = "oracle+{}://{}:{}@{}".format(driver, user, password, dns.replace('SID', 'SERVICE_NAME'))
    return connstr, schema


def load_weather_data():
    connstr, schema = db_config()
    logging.info("Weather data Ingestion started at : {}".format(time.strftime("%H:%M:%S", time.localtime())))
    engine = create_engine(connstr)
    csv_files = glob.glob(os.path.join(BASE_DIR / 'wx_data', "*.txt"))
    total_records = 0
    # loop over the list of csv files
    for f in csv_files:
        # read the csv file
        logging.info('{} file process started at: {} '.format(f.split("\\")[-1],
                                                              time.strftime("%H:%M:%S", time.localtime())))
        df = pd.read_csv(f, sep='\t', header=None)
        df['WEATHER_STATION'] = f.split("\\")[-1][0:-4]
        df.columns = ['CREATED_DATE', 'MAX_TEMP', 'MIN_TEMP', 'PRECIPITATION', 'WEATHER_STATION']
        df = df[['WEATHER_STATION', 'CREATED_DATE', 'MAX_TEMP', 'MIN_TEMP', 'PRECIPITATION']]
        df['CREATED_DATE'] = pd.to_datetime(df['CREATED_DATE'], format='%Y%m%d')
        df = df.replace(to_replace=-9999, value=None)
        df = df.drop_duplicates()
        row_count = 0
        try:
            row_count = df.to_sql('weatherdata', con=engine, index=False, schema=schema, if_exists='append')
            total_records = total_records + row_count
        except sqlalchemy.exc.IntegrityError:
            logging.info('{} was already processed'.format(f.split("\\")[-1][0:-4]))
        except BaseException as e:
            logging.warning('Not able to connect to database')
        else:
            end_time = time.strftime("%H:%M:%S", time.localtime())
            logging.info('{} file process ended at: {}, Records Inserted:{} '.format(f.split("\\")[-1],
                                                                                     end_time,
                                                                                     row_count))
    end_time = time.strftime("%H:%M:%S", time.localtime())
    logging.info("Weather data Ingestion ended at : {} , Total Records Inserted:{}".format(end_time, total_records))


def load_yield_data():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    logging.info("Yield data Ingestion started at : {}".format(time.strftime("%H:%M:%S", time.localtime())))
    csv_files = glob.glob(os.path.join(BASE_DIR / 'yld_data', "*.txt"))
    # loop over the list of csv files
    for f in csv_files:
        # read the csv file
        logging.info('{} file process started at: {} '.format(f.split("\\")[-1],
                                                              time.strftime("%H:%M:%S", time.localtime())))
        df = pd.read_csv(f, sep='\t', header=None)
        df.columns = ['YIELD_YEAR', 'TOTAL_YIELD']
        row_count = 0
        df = df.drop_duplicates()
        try:
            row_count = df.to_sql('yielddata', con=engine, index=False, schema=schema, if_exists='append')
        except sqlalchemy.exc.IntegrityError as e:
            logging.warning('{} was already processed'.format(f.split("\\")[-1][0:-4]))
        except BaseException as e:
            logging.info('Cannot connect to database')
        else:
            logging.info('{} file process ended at: {}, Records Inserted:{} '.format(f.split("\\")[-1],
                                                                                     time.strftime("%H:%M:%S",
                                                                                                   time.localtime()),
                                                                                     row_count))
    logging.info("Yield data Ingestion ended at : {}".format(time.strftime("%H:%M:%S",
                                                                           time.localtime())))


def load_statistics():
    connstr, schema = db_config()
    engine = create_engine(connstr)
    st_time = time.strftime("%H:%M:%S", time.localtime())
    logging.info("Statistics data Ingestion started at : {}".format(st_time))
    try:
        with engine.connect() as con:
            query_string = """MERGE INTO STATISTICS D using (select WEATHER_STATION,
            extract(year from created_date) yield_year,
            avg(max_temp)/10  avg_max_temp,avg(min_temp)/10  avg_min_temp,
            sum(PRECIPITATION)/100  total_precipitation from weatherdata 
            group by WEATHER_STATION,extract(year from created_date)) S 
            ON (D.WEATHER_STATION=S.WEATHER_STATION and D.yield_year=S.yield_year) 
            WHEN MATCHED THEN UPDATE SET D.avg_max_temp=S.avg_max_temp,
            D.avg_min_temp=S.avg_min_temp, D.total_precipitation=S.total_precipitation 
            WHEN NOT MATCHED THEN  
            INSERT (D.WEATHER_STATION,D.yield_year,D.avg_max_temp,D.avg_min_temp, 
            D.total_precipitation) 
            VALUES(S.WEATHER_STATION,S.yield_year,S.avg_max_temp,S.avg_min_temp, 
            S.total_precipitation)"""
            query = con.execute(text(query_string).execution_options(autocommit=True))
            logging.info("Records Loaded to statistics table : {}".format(query.rowcount))
    except sqlalchemy.exc.IntegrityError as e:
        logging.warning('Statistics already processed')
    except BaseException as e:
        logging.info('Unable to process the file')
    else:
        end_time = time.strftime("%H:%M:%S", time.localtime())
        logging.info('Statistics data ingestion ended at: {}, Records Inserted:{} '.format(end_time,
                                                                                           query.rowcount))


if __name__ == "__main__":
    load_yield_data()
    load_weather_data()
    load_statistics()