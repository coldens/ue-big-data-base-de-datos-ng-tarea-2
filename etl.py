import pandas as pd

from etl_functions import income_by_client, run_etl, get_client_buys, top_3_clients

# Index(['Nombre,Apellido,Email,Producto,Precio'], dtype='object')
df = pd.read_excel("./clientes-productos.csv")

# Split columns 'Nombre,Apellido,Email,Producto,Precio' into columns
df[["Nombre", "Apellido", "Email", "Producto", "Precio"]] = df[
    "Nombre,Apellido,Email,Producto,Precio"
].str.split(",", expand=True)

df = df.drop(columns=["Nombre,Apellido,Email,Producto,Precio"])
df = df.sort_values(by=["Email"])

clients = df[["Email", "Nombre", "Apellido"]].reset_index(drop=True)
products = df[["Producto", "Precio"]].drop_duplicates().reset_index(drop=True)
buys = df[["Email", "Producto"]].drop_duplicates().reset_index(drop=True)

clients["id"] = clients.index
products["id"] = products.index
buys["id"] = buys.index

clients["id"] = clients["id"] + 1
products["id"] = products["id"] + 1
buys["id"] = buys["id"] + 1


run_etl(clients, products, buys)

print("Buys for 'alba.lara@example.com':")
print(get_client_buys('alba.lara@example.com'))
print("Top 3 Clients:")
print(top_3_clients())

print("Income by Client:")
print(income_by_client())