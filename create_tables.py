import mysql.connector

#Define mysql database credentials
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="@admin#2024*10",
    database="batch_streaming",
)

#Open the sql file for creating tables
with open("tables.sql", "r") as file:
    tables = file.read()
    

#Execute the sql file to create tables
cursor = connection.cursor()
for result in cursor.execute(tables, multi=True):
    if result.with_rows:
        print(f"Selected {result.rowcount} row(s)")
    else:
        print(f"Affected {result.rowcount} row(s)")


