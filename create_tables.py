import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """ Here we drop the tables if they exist """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ Here we create the tables if they dont exist """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def main():
    """ Here we parse the config file values and connect to the AWS redshift custer \
    as well executing the functions above """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try: 
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        
        cur = conn.cursor()
    except e:
        print(e)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()