import psycopg2
import os
import pandas as pd 
from sqlalchemy import create_engine

# conn = psycopg2.connect(
#             host = "localhost",
#             database = "postgres",
#             user = "postgres",
#             password = "baseball")

df = pd.read_csv('C:/Users/Shane/Documents/PBA Stat Files/Pitchers.csv', engine='python')

engine = create_engine('postgresql://postgres:baseball@localhost:5432/postgres')
df.to_sql('pitchers', engine, if_exists='replace')

# try:
#     cur = conn.cursor()
# except psycopg2.Error as e:
#     print("Error: Could not get cursor to the Database")
#     print(e)

# conn.set_session(autocommit=True)

# try:
#     cur.execute("CREATE TABLE IF NOT EXISTS pitchers (POS varchar, Jersey_num varchar, Name varchar, WAR int);")
# except psycopg2.Error as e:
#     print("Error: Issue creating table")
#     print(e)

# try:
#     cur.execute("select count(*) from music_library")
# except psycopg2.Error as e:
#     print("Error: Issue creating table")
#     print(e)
    
# print(cur.fetchall())

# try:
#     cur.execute("INSERT INTO pitchers (POS, Jersey_num, Name, WAR) \
#                 VALUES (%s, %s, %s, %s)", \
#                 ("Let It Be", "The Beatles", 1970))
# except psycopg2.Error as e:
#     print("Error: Inserting Rows")
#     print(e)

# try:
#     cur.execute("INSERT INTO music_library (album_name, artist_name, year) \
#                 VALUES (%s, %s, %s)", \
#                 ("Rubber Soul", "The Beatles", 1965))
# except psycopg2.Error as e:
#     print("Error: Inserting Rows")
#     print(e)

# try:
#     cur.execute("SELECT * FROM music_library;")
# except psycopg2.Error as e:
#     print("Error: select *")
#     print(e)

# row = cur.fetchone()
# while row:
#     print(row)
#     row = cur.fetchone()

# cur.execute("DROP TABLE pitchers")

# cur.close()
# conn.close()
