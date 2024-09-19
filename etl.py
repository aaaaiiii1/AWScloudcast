import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables with data from the JSON files.
    Args:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into the final tables from the staging tables.
    Args:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that orchestrates the ETL process.
    This function calls the `process_data` function to process the song and log data,
    and then loads the processed data into the Redshift database.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    """
    Loads the staging tables with data from the JSON files.
    Args:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    load_staging_tables(cur, conn)
    """
    Inserts data into the final tables from the staging tables.
    Args:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()