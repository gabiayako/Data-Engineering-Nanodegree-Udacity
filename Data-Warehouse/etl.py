import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function executes queries that copy data from S3 files to amazon redshift tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function executes queries that select data from staging tables and insert on dimension/fact tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads dwh.cfg file

    - Establishes connection with a Redshift database
    
    - Loads data from S3 to Redshift 
    
    - Inserts data on Redsift tables
    
    - Finally, closes the connection. 
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()