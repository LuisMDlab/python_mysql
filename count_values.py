#Imports - Are utilized pandas for read sql and write in csv, as well sqlalchemy for database connection.
import pandas as pd
from sqlalchemy import create_engine

#Create a connection
engine = create_engine('mysql+pymysql://user:password@host:3306/{}?charset=utf8'.format('database_name'))
connection = engine.connect()

#SQL query to list all tables of database
tables_from_db = pd.read_sql_query("""SHOW TABLES;""", con = connection)
tables_described = {}

#Function to count, with a SQL query, all values from a column
def describe_table(table):
    
    table_df = pd.read_sql_table(table, con = connection)
    items_dict = {}
    
    for column in table_df.columns:
        query_transform = """SELECT COUNT({}) as contagem FROM {} WHERE {} != '';""".format(str(column), str(table), str(column))
        consulta = pd.read_sql_query(query_transform, con=connection)
        items_dict[table + '.' + column] = consulta['contagem']
        
    return items_dict

#Runs the function for all tables of the database.
for tableDb in tables_from_db['Tables_in_db']:
    print('Describing table: {}'.format(tableDb))
    tables_described[tableDb] = describe_table(tableDb)

#Export dataframe with values to csv.
descriptionTables = pd.DataFrame.from_dict(tables_described)
descriptionTables.to_csv('descricao_tabelas(try).csv', encoding = 'utf-8')

#The output structure of colmns aren't cool iet. As soon as i can, i will change that!
