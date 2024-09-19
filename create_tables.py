import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all the tables in the database by executing the drop queries in `drop_table_queries`.
    Parameters:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all the tables in the database by executing the create queries in `create_table_queries`.
    Parameters:
        cur (psycopg2 cursor): The cursor object to execute SQL queries.
        conn (psycopg2 connection): The connection object to the Redshift database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that orchestrates the database setup process.
    It reads the database configuration from 'dwh.cfg'
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    """establishes a connection to the Redshift database"""
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    """drops existing tables, creates new tables"""
    drop_tables(cur, conn)
    create_tables(cur, conn)
    """and then closes the connection"""
    conn.close()


if __name__ == "__main__":
    main()