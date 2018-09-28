# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 08:57:50 2018

@author: Luis
"""
import pandas as pd
from sqlalchemy import create_engine
import csv

class museum_treat():
    "Class to treat brazilian museums databases."
    
    def sql_multivalue(db_credentials,end_table_name,query, from_db_name, end_db_name, column_id):
        
        "->Treat multivalues from a sql table, and create another table with the values separated by ||\n\
        \n#db_credentials -> Acces credentials for database in format 'user:password@server'.\
        \n#end_table_name -> the name of table that will be crated.\
        \n#query -> query that joins itens id and de multivalued fields.\
        \n#from_db_name -> name of database where query has been made.\
        \n#end_db_name -> name of database where table with multivalues will be inputed.\
        \n#column_id -> name of the column where item id is."
        
        #Connection with db -> SQLAlquemy.
        engine = create_engine('mysql+pymysql://{}/{}?charset=utf8'.format(db_credentials,from_db_name))
        connection = engine.connect()
        
        #Get the fist part of the function, to produce a list of itens id.
        print("Working on table {}".format(end_table_name))
        query_treatment = query.split('WHERE')
        query_treatment = query_treatment[0] + ';'
        queryDocs_df = pd.read_sql_query(query_treatment, con=connection)
        
        #The name of column where item ID is must be dfined.
        lista_docs = (doc for doc in queryDocs_df[column_id])
        lista_docs = set(lista_docs)
        
        cont = 1
        for doc in lista_docs:
            
            
            print("Inserting document {}".format(doc))
           
            #Utiliza a query de junção das tabelas.
            query_df = pd.read_sql_query(query.format(doc), con=connection)
            result_dict={}
            query_dict={}
    
            #Para cada coluna da resultante da query de junção, cria um dicionário com o nome da coluna(key), e uma lista para seus valores.
            for column in query_df.columns:
                query_dict[column] = []

                #Adiciona os valores resultantes na lista.
                for iten in query_df[column]:
                    if iten == None or pd.isna(iten):
                        continue
                    else:
                        query_dict[column].append(str(iten))
    
            query_dict[column] = set(query_dict[column])
        
            result_dict[column] = ['||'.join(query_dict[column])]
            
            result_df = pd.DataFrame.from_dict(result_dict, orient='columns')
                        
            result_df.to_sql(name=end_table_name, con=connection, schema=end_db_name, if_exists='append', index=False)
            
            print("{} Documents Remain!".format(len(lista_docs) - cont))
            
            cont+=1
            
    def csv_to_sql(db_credentials, csv_file, db_name, table_name):
        "->CSV file to sql table.\n\
        \n#db_credentials -> Acces credentials for database in format 'user:password@server'.\
        \n#csv_file -> name or path of csv_file.\
        \n#db_name -> name of database to use.\
        \n#table_name -> name of tbale to create and input csv data."
            
        engine = create_engine('mysql+pymysql://{}/{}?charset=utf8'.format(db_credentials, db_name))
        connection = engine.connect()
        print("Start consersion of csv file, to sql table {}".format(table_name))
        input_data = pd.read_csv(csv_file, encoding='utf-8')
        input_data.to_sql(name=table_name, con = connection, schema=db_name, index=False)
        print("Success!!")
           
    def sql_to_csv(db_credentials, db_name, query, csv_file):
           
        "->SQL Query to CSV file.\n\
        \n#db_credentials -> Acces credentials for database in format 'user:password@server'.\
        \n#db_name -> name of database to use.\
        \n#csv_file -> name or path of csv_file.\
        \n#query -> SQL Query. To convert entire table, just put the select table."

        engine = create_engine('mysql+pymysql://{}/{}?charset=utf8'.format(db_credentials, db_name))
        connection = engine.connect()

        df = pd.read_sql_query(query, con=connection, coerce_float =False)
        with open(csv_file, "w", newline='', encoding='utf-8') as csv_file:  # Python 3 version
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(df.columns) # write headers
            print("Wrinting line")
            for i in range(len(df)):
                print("Wrinting line {}".format(i))
                print("{} Remain...".format(len(df)-(i+1)))
                csv_writer.writerow(df.iloc[i])          
