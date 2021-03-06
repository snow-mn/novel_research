# 形態素解析を行い、データベースに形態素解析情報を格納するプログラム
# ストップリストを除外してデータベースに格納する

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

# 格納するデータベース名
database_name = "ma_data_not_stop"

# DataFrameの列名
df_columns = ["ncode", "line_index", "token_index", "token_text", "token_lemma", "token_pos", "token_tag",
              "token_dep", "token_norm", "token_ent_iob", "token_ent_type", "token_is_oov", "token_sent"]


# postgreSQLからデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


# 既に形態素解析データがPostgreSQLに格納されているかの判定
def check_existed(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE ncode = '%s');" % (database_name, ncode))
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# 形態素解析
def morphological_analysis(connection, ncode):
    # PostgreSQLからDataFrameを取得
    df = pd.read_sql(sql="SELECT honbun FROM text_data WHERE ncode='%s';" % ncode, con=connection)
    # DataFrameから本文データを取り出す
    lines = df["honbun"][0]
    # ginzaの準備
    nlp = spacy.load("ja_ginza")
    # 何行目かカウントする変数
    line_num = 0
    # データを入れるリスト
    token_data = []

    # 1行ごとに形態素解析を行う
    for line in tqdm(lines):
        doc = nlp(line)
        # 各トークンの情報を取得
        token_data.extend([[ncode, line_num, tok.i, tok.lemma_, tok.pos_, tok.tag_, tok.dep_, tok.norm_, tok.ent_iob,
                      tok.ent_type_, tok.is_oov, str(tok.sent)] for tok in doc if not tok.is_stop])
        # 行数カウントを1増やす
        line_num += 1

    print("リストをDataFrameに変換しています")
    # リストをDataFrame化
    ma_df = pd.DataFrame(token_data, columns=df_columns)
    print(ma_df)
    return ma_df


# データベースに格納する関数
def save_postgresql(ma_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    ma_df.to_sql(database_name, con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからメタデータの取得
    df = get_postgresql_data(connection)
    # データリストからncodeを順々に読み込んでいく
    for ncode in df["ncode"]:
        # 該当作品の形態素解析データが既にデータベースに存在していなければ
        if not check_existed(connection, ncode)[0]:
            print("%sの形態素解析を開始します" % ncode)
            ma_df = morphological_analysis(connection, ncode)
            # PostgreSQLに形態素解析データを格納
            save_postgresql(ma_df)
        else:
            print("%sの形態素解析データは既にデータベースに存在しています" % ncode)


# 実行
if __name__ == '__main__':
    main()