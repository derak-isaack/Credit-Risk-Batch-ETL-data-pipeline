import mysql.connector
import psycopg2


connect = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Admin",
    database="data_streams",
    port = 5432
)

#Open the sql file for creating tables
with open("tables.sql", "r") as file:
    tables = file.read()
    

#Execute the sql file to create tables
cursor = connect.cursor()
for statement in tables.split(';'):
        statement = statement.strip()
        if statement:
            cursor.execute(statement)
            
connect.commit()
   
#Close the connection to avoid errors when writing data to postgres     
cursor.close()


