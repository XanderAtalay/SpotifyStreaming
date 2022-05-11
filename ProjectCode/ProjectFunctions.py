def get_lyrics(song, artist):
    try:
        song = genius.search_song(song, artist)
        
        rawLyrics = song.lyrics
        lyrics = re.sub(r"\[.*?\]", "", rawLyrics)
        lyrics = lyrics.strip().split("\n",1)[1].replace('\n', ' ')
        return lyrics
    
    except:
        return None
    
# The following functions were provided in DS3002 Lab 3

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