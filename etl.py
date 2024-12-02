"""Run this code for etl pipelines to load data into staging and final tables."""

import configparser
import psycopg2
from sql_queries import insert_table_queries, copy_table_queries

def load_staging_tables(cur, conn):
    """Load staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data into final tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connect to redshift cluster, insert data into staging and final tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()