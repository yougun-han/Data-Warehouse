import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy the staging data using the queries in `copy_table_queries` list.
    """ 
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert the data into fact and dimensional tables using the queries in `insert_table_queries` list.
    """ 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Read config the file "dwh.cfg"
    - Establishes connection with the sparkify database and gets cursor to it.   
    - Copy the staging data.  
    - Insert the data into fact and dimensional tables. 
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