import pandas as pd
import psycopg2

DB_NAME = "cexplorer"
DB_HOST = "/var/run/postgresql"
DB_USER = "siri"
DB_PASS = "PasswordYouWant"
DB_PORT = "5432"

conn = psycopg2.connect(
    dbname=DB_NAME,
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    port=DB_PORT
)

# define cursor
cur = conn.cursor()


def query(sql):
    """
    执行一个查询，并返回查询结果
    """
    cur.execute(sql)
    all = cur.fetchall()
    return list(all)


def query_as_pd(sql):
    """
    执行一个查询，并返回 DataFrame
    """
    result = query(sql)
    return pd.DataFrame(result)
