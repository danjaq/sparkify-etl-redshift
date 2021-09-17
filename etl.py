import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads the staging table from the S3 buckets indicated in the 
    config file."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts the data from the staging tables into the final
    star scheme tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Main function loading config values, making a connection and
    calling the loading and inserting functions."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading Staging Tables...")
    load_staging_tables(cur, conn)
    print("Inserting into Final Tables...")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()