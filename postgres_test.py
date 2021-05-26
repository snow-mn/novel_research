import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# データベースの接続情報
connection_config = {
    "host": "localhost",
    "port": "5432",
    "dbname": "narou_data",
    "user": "postgres",
    "password": "password"
}


# データベースへ接続
def get_connection():
    con = psycopg2.connect(**connection_config)
    return con


# データベースへ接続2
def  get_connection2():
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    df.to_sql('テーブル名', con=engine, if_exists='append', index=False)


# PostgreSQLに書き込む
def append_postgres():
    # PostgreSQLのテーブルにDataFrameを追加する
    df.to_sql('テーブル名', con=engine, if_exists='append', index=False)


# SQL文を実行
def execute_sql(con, sql):
    cur = con.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results


# テーブルの全レコードを取得
def get_all_record(con, tablename):
    with con.cursor() as cur:
        cur.execute("SELECT * FROM %s" % tablename)
        rows = cur.fetchall()



# メイン関数
if __name__ == "__main__":
    # データベースへ接続
    con = get_connection()
    # テーブル"metadata"の全レコードを取得
    get_all_record(con, "metadata")