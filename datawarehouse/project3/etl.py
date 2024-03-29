import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to the staging tables using the queries declared on the sql_queries script
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Select and Transform data from staging tables into the dimensional tables using
    the queries declared on the sql_queries script
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Extract songs metadata and user activity data from S3, transform it using a staging table, and 
    load it into dimensional tables for  analysis
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