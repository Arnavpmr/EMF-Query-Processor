import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv


def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT cust, prod, avg(quant) FROM sales WHERE state='NJ' GROUP BY cust, prod HAVING sum(quant) > 15000")

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
