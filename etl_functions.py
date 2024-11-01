import random

from mydb import commit, get_cursor

""" Tablas:

Clientes
ID_Cliente (Primary Key)
Nombre
Apellido
Email

Productos
ID_Producto (Primary Key)
Nombre_Producto
Precio

Compras
ID_Compra (Primary Key)
ID_Cliente (Foreign Key)
ID_Producto (Foreign Key)
Fecha_Compra """


def run_etl(clients, products, buys):
    mycursor = get_cursor()

    # create database if not exists
    mycursor.execute("CREATE DATABASE IF NOT EXISTS tarea1")

    # use database
    mycursor.execute("USE tarea1")

    # create table Clientes if not exists
    mycursor.execute(
        "CREATE TABLE IF NOT EXISTS Clientes (ID_Cliente INT PRIMARY KEY, Nombre VARCHAR(50), Apellido VARCHAR(50), Email VARCHAR(50))"
    )

    # create table Productos if not exists
    mycursor.execute(
        "CREATE TABLE IF NOT EXISTS Productos (ID_Producto INT PRIMARY KEY, Nombre_Producto VARCHAR(50), Precio DECIMAL(10,2))"
    )

    # create table Compras if not exists
    mycursor.execute(
        "CREATE TABLE IF NOT EXISTS Compras (ID_Compra INT PRIMARY KEY, ID_Cliente INT, ID_Producto INT, Fecha_Compra DATE)"
    )

    # truncate table Clientes
    mycursor.execute("TRUNCATE TABLE Clientes")

    # truncate table Productos
    mycursor.execute("TRUNCATE TABLE Productos")

    # truncate table Compras
    mycursor.execute("TRUNCATE TABLE Compras")

    commit()

    # insert data into Clientes
    for index, row in clients.iterrows():
        mycursor.execute(
            "INSERT INTO Clientes (ID_Cliente, Nombre, Apellido, Email) VALUES (%s, %s, %s, %s)",
            (row["id"], row["Nombre"], row["Apellido"], row["Email"]),
        )

    commit()

    # insert data into Productos
    for index, row in products.iterrows():
        mycursor.execute(
            "INSERT INTO Productos (ID_Producto, Nombre_Producto, Precio) VALUES (%s, %s, %s)",
            (row["id"], row["Producto"], row["Precio"]),
        )

    commit()

    # insert data into Compras
    for index, row in buys.iterrows():
        # Random Date
        Month = random.randint(1, 12)
        Day = random.randint(1, 28)
        Year = random.randint(2022, 2024)

        # Mysql Format Date
        Fecha_Compra = f"{Year}-{Month:02d}-{Day:02d}"

        cliente = clients.loc[clients["Email"] == row["Email"]]
        producto = products.loc[products["Producto"] == row["Producto"]]

        ID_Cliente = int(cliente["id"].values[0])
        ID_Producto = int(producto["id"].values[0])
        mycursor.execute(
            "INSERT INTO Compras (ID_Compra, ID_Cliente, ID_Producto, Fecha_Compra) VALUES (%s, %s, %s, %s)",
            (row["id"], ID_Cliente, ID_Producto, Fecha_Compra),
        )

    commit()


def get_client_buys(Email):
    mycursor = get_cursor()

    # select data from Compras
    mycursor.execute(
        "SELECT p.Nombre_Producto, p.Precio, cl.Nombre, cl.Apellido, cl.Email, c.Fecha_Compra FROM Compras c JOIN Clientes cl ON c.ID_Cliente = cl.ID_Cliente JOIN Productos p ON c.ID_Producto = p.ID_Producto WHERE cl.Email = %s",
        (Email,),
    )

    # fetch all as a list of dictionaries
    buys = mycursor.fetchall()
    mycursor.close()
    return buys

def top_3_clients():
    mycursor = get_cursor()

    # select data from Clientes
    mycursor.execute(
        "SELECT cl.Nombre, cl.Apellido, cl.Email, COUNT(c.ID_Compra) as 'Compras' FROM Compras c JOIN Clientes cl ON c.ID_Cliente = cl.ID_Cliente GROUP BY c.ID_Cliente ORDER BY 'Compras' DESC LIMIT 3"
    )

    # fetch all as a list of dictionaries
    clients = mycursor.fetchall()
    mycursor.close()
    return clients

def income_by_client():
    mycursor = get_cursor()

    # select data from Clientes
    mycursor.execute(
        "SELECT cl.Nombre, cl.Apellido, cl.Email, SUM(p.Precio * c.Fecha_Compra) as 'Total' FROM Compras c JOIN Clientes cl ON c.ID_Cliente = cl.ID_Cliente JOIN Productos p ON c.ID_Producto = p.ID_Producto GROUP BY c.ID_Cliente ORDER BY 'Total' DESC"
    )

    # fetch all as a list of dictionaries
    clients = mycursor.fetchall()
    mycursor.close()
    return clients