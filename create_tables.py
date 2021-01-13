import configparser
import psycopg2 as pg
from queries import create_table_queries, drop_table_queries
from utils import *

def drop_tables(cur, conn):
    """
    Drop tables in the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Create tables in the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create tables in the database
    """
    conn = connect_to_redshift()
    cur = conn.cursor()

    print("Droping tables in the database...")
    drop_tables(cur, conn)

    print("Creating tables in the database...")
    create_tables(cur, conn)
    
    conn.close()
    
if __name__ == "__main__":
    main()
