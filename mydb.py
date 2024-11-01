import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",  # we should use a user with less privileges than root
    password="test",  # this should be get from a secret manager
)

def get_cursor():
    return mydb.cursor()

def close_connection():
    mydb.close()

def commit():
    mydb.commit()