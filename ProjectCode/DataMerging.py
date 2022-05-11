#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from ProjectFunctions import *
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import json
import pymongo
from datetime import datetime
import os

currentTime = str(datetime.now()).replace(" ", "T")[:(len(str(datetime.now()).replace(" ", "T")) - 7)]

def get_dataframe(user_id, pwd, host_name, db_name, sql_query):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    dframe = pd.read_sql(sql_query, connection);
    connection.close()
    
    return dframe


def set_dataframe(user_id, pwd, host_name, db_name, df, table_name, pk_column, db_operation):
    conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}/{db_name}"
    
    sqlEngine = create_engine(conn_str, pool_recycle=3600)
    connection = sqlEngine.connect()
    
    if db_operation == "insert":
        df.to_sql(table_name, con=connection, index=False, if_exists='replace')
        sqlEngine.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_column});")
            
    elif db_operation == "update":
        df.to_sql(table_name, con=connection, index=False, if_exists='append')
    
    connection.close()
    
host_name = "localhost"
host_ip = "10.0.0.217"
port = "3306"

user_id = "root"
pwd = "Xander22"
dst_database = "CurrentSpotifyFeatured"
src_database = "AllTimeSpotifyFeatured"

conn_str = f"mysql+pymysql://{user_id}:{pwd}@{host_name}"
sqlEngine = create_engine(conn_str, pool_recycle=3600)

sql_currentFeaturedTable = "SELECT * FROM CurrentSpotifyFeatured.currentFeatured;"
currentFeatured = get_dataframe(user_id, pwd, host_name, dst_database, sql_currentFeaturedTable).iloc[: , 1:].drop_duplicates()

# Dropping SQL Current Featured Database
exec_sql = f"DROP DATABASE `{dst_database}`;"
sqlEngine.execute(exec_sql) #remove current featured database

# Adding CurrentFeatured.Json to a history directory
currentFeatured.to_json(path_or_buf = ("./CurrentFeaturedHistoryTables/SpotifyFeatured" + currentTime + ".json"),
                       orient = "records")

def get_mongo_dataframe(user_id, pwd, host_name, port, db_name, collection, query):
    '''Create a connection to MongoDB, with or without authentication credentials'''
    if user_id and pwd:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
        client = pymongo.MongoClient(mongo_uri)
    else:
        conn_str = f"mongodb://{host_name}:{port}/"
        client = pymongo.MongoClient(conn_str)
    
    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    dframe = pd.DataFrame(list(db[collection].find(query)))
    dframe.drop(['_id'], axis=1, inplace=True)
    client.close()
    
    return dframe

host_name = "localhost"
mongo_port = 27017

user_id = "root"
pwd = "Xander22"

src_dbname = "CurrentSpotifyFeatured"

conn_str = f"mongodb://{host_name}:{mongo_port}/"
client = pymongo.MongoClient(conn_str)
db = client[src_dbname]

CurrentFile = "./CurrentFeaturedHistoryTables/SpotifyFeatured" + currentTime + ".json"
AlltimeFile = "SpotifyFeaturedMaster.json"

# for file in json_files:
#     json_file = os.path.join(data_dir, json_files[file])
#     with open(json_file, 'r') as openfile:
#         json_object = json.load(openfile)
#         file = db[file]
#         result = file.insert_many(json_object)
#         #print(f"{file} was successfully loaded.")
        
if os.path.exists(os.path.join(os.getcwd(), "SpotifyFeaturedMaster.json")):
    with open(AlltimeFile, 'r') as openfile:
        json_object = json.load(openfile)
        file = db['AlltimeFeatured']
        result = file.insert_many(json_object)
    
with open(CurrentFile, 'r') as openfile:
    json_object = json.load(openfile)
    fileI = db['currentFeatured']
    fileII = db['AlltimeFeatured']
    resultI = fileI.insert_many(json_object)
    resultII = fileII.insert_many(json_object)
    
    
client.close()

query = {}

collection = "AlltimeFeatured"
AllTimeFeatured = get_mongo_dataframe(None, None, host_name, mongo_port, src_dbname, collection, query)

# Adding AllTimeFeatured.json to a history directory
AllTimeFeatured.to_json(path_or_buf = ("./AlltimeFeaturedHistoryTables/AllTimeFeatured" + currentTime + ".json"),
                       orient = "records")

# Writing AllTimeFeatured.json to a the current directory
AllTimeFeatured.to_json(path_or_buf = ("./SpotifyFeaturedMaster.json"),
                       orient = "records")

result = client.drop_database("CurrentSpotifyFeatured")

