from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData
import pymysql

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here

while True:
    try:
        print(environ["MYSQL_CS"])
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()
        devices = Table(
            'devices', String,
            Column('device_id', String),
            Column('temperature', Integer),
            Column('location', String),
            Column('time', String),
        )
        metadata_obj.create_all(mysql_engine)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')