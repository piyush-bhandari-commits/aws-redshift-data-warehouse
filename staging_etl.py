import configparser
import psycopg2 as pg
from queries import copy_table_queries, insert_table_queries
from utils import connect_to_database

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Executing query : {}'.format(query))
        cur.execute(query)
        conn.commit()
    print("All tables loaded...")

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Executing query : {}'.format(query))
        cur.execute(query)
        conn.commit()
    print("All tables inserted in the database...")


def main():
    
    conn = connect_to_database()
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()