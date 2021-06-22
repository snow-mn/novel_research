# ncodeとキーワードの一対一の対応関係をデータベースに格納するプログラム

import spacy
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from tqdm import tqdm


# データベースの接続情報
connection_config = {
    "host": "localhost",
    "port": "5432",
    "dbname": "narou_data",
    "user": "postgres",
    "password": "password"
}


# postgreSQLからデータを取得する関数
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    # df = pd.read_sql(sql="SELECT ncode, keyword FROM metadata ORDER BY global_point DESC;", con=connection)
    # テキストデータが存在するものに絞って取得
    df = pd.read_sql(sql="SELECT ncode, keyword FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


# データベースから一旦データを削除する関数
def delete_data(connection):
    print("以前のデータを削除します")
    # データベースからデータを削除するsql文
    query = "DELETE FROM keyword_data;"
    # カーソルを生成
    with connection.cursor() as cur:
        # sql文を実行
        cur.execute(query)
        # コミット
        connection.commit()
        print("以前のデータが削除されました")


# データベースに格納する関数
def save_postgresql(keyword_df):
    print("データベースにデータを保存します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    keyword_df.to_sql("keyword_data", con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからメタデータの取得
    df = get_postgresql_data(connection)
    # DataFrameをリストに変換
    df_list = df.to_numpy().tolist()
    # ncodeとキーワードの一対一の対応関係を入れるリスト
    ncode_keyword_list = []
    # リストを順々に読み込んでいく
    for ncode, keyword_list in tqdm(df_list):
        # 重複判定のための仮のリスト
        kari_list = []
        # キーワードリストからキーワードを一つずつ読み込んでいく
        for keyword in keyword_list:
            # キーワードが空白でなく、ncodeとキーワードの組が重複していないなら
            if keyword != "" and [ncode, keyword] not in kari_list:
                # リストに追加
                ncode_keyword_list.append([ncode, keyword])
                # 重複判定のために仮のリストに追加
                kari_list.append([ncode, keyword])
            else:
                pass
    # リストをDataFrameに変換
    keyword_df = pd.DataFrame(ncode_keyword_list, columns=["ncode", "keyword"])
    print(keyword_df)
    # 以前のデータを削除
    delete_data(connection)
    # PostgreSQLに格納
    save_postgresql(keyword_df)


# 実行
if __name__ == '__main__':
    main()