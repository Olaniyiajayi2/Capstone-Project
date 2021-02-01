import configparser
import psycopg2
from sql_queries import *



def load_staging_tables(cur, conn):
    """
    This function loads in staging table
    Parameters
    -------------------
    cur:- Database Cursor
    
    conn:- Database connector
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
        
        
def insert_tables(cur, conn):
    """
    This function inserts tables
    Parameters
    -------------------
    cur:- Database Cursor
    
    conn:- Database connector
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    config = configparser.ConfigParser()
    config.read('conf1.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("[INFO] Connection to database done...")
    
    print("[INFO] loading tables into redshift...")
    load_staging_tables(cur, conn)
    print("[INFO] Tables loaded...")
    
    print("[INFO] Inserting tables...")
    insert_tables(cur, conn)
    print("[INFO] Tables loaded...")
    
    print("[INFO] Closing connection...")
    conn.close()
    print("[INFO] Done...")
    
    
if __name__ == "__main__":
    main()