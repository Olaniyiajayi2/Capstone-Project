import configparser
import psycopg2
from sql_queries import *

def drop_table_if_exist(cur, conn):
    """
    This function drops tables if they already exist
    Parameters
    -------------------
    cur:- Database Cursor
    
    conn:- Database connector
    """
    for query in drop_table_queries:
        print("[INFO] dropping tables if exist:" + query)
        cur.execute(query)
        conn.commit()
        
        
        
def create_tables(cur, conn):
    """
    This function drops tables if they already exist
    Parameters
    -------------------
    cur:- Database Cursor
    
    conn:- Database connector
    """
    for query in create_table_queries:
        print("[INFO] creating tables:" + query)
        cur.execute(query)
        conn.commit()
        

    
def main():
    config = configparser.ConfigParser()
    config.read('conf.cfg')
    
    print("Connecting remotely to redshift")
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Successfully Connected to AWS redshift")

    cur = conn.cursor()

    print("Dropping tables if exist")
    drop_table_if_exist(cur, conn)

    print("Creating tables")
    create_tables(cur, conn)

    #close connection
    conn.close()
    print("Tables successfully created")

if __name__ == "__main__":
    main()