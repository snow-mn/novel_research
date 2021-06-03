# 形態素解析を行い、データベースに形態素解析情報を格納するプログラム

import spacy
import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from tqdm import tqdm

# テキストファイルのパス
text_dir = "../novel_Text/"

# データベースの接続情報
connection_config = {
    "host": "localhost",
    "port": "5432",
    "dbname": "narou_data",
    "user": "postgres",
    "password": "password"
}

# DataFrameの列名
# [ncode, line_num, tok.i, tok.text, tok.orth_, tok.lemma_, tok.pos_, tok.tag_, tok.head.i, tok.dep_, tok.norm_, tok.ent_.iob_, tok.ent_.type_]
df_columns = ["ncode", "line_num", "tok_index", "tok_text", "tok_orth", "tok_lemma", "tok_pos", "tok_tag", "tok_head_index", "tok_dep", "tok_norm", "tok_ent_iob", "tok_ent_type"]


# postgreSQLからデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


# 既に形態素解析データがPostgreSQLに格納されているかの判定
def check_existed(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM ma_data WHERE ncode = '%s');" % ncode)
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# 形態素解析
def morphological_analysis(ncode, lines):
    # ginzaの準備
    nlp = spacy.load("ja_ginza")
    # DataFrameの作成
    ma_df = pd.DataFrame(columns=df_columns)
    # 何行目かカウントする変数
    line_num = 0
    # 1行ごとに形態素解析を行う
    for line in tqdm.tqdm(lines):
        doc = nlp(line)
        # 各トークンの情報を取得
        for tok in doc:
            # トークン情報をまとめたリスト
            token_info = [ncode, line_num, tok.i, tok.text, tok.orth_, tok.lemma_, tok.pos_, tok.tag_, tok.head.i, tok.dep_, tok.norm_, tok.ent_.iob_, tok.ent_.type_]
            # DataFrameのline_num行目に追加
            ma_df.loc[line_num] = token_info
        # 行数カウントを1増やす
        line_num += 1
    print(ma_df)
    dump_to_postgresql(ncode, ma_df)


# データベースに格納する関数
def dump_to_postgresql(ncode, ma_df):
    # DataFrameの作成
    df = pd.DataFrame(columns=["ncode", "honbun"])
    # DataFrameに追加
    df.loc[0] = [ncode, lines]
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    df.to_sql("ma_data", con=engine, if_exists='append', index=False)
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
            print("%sの小説本文のダウンロードを開始します" % ncode)
            ma_df = morphological_analysis(ncode)
            # PostgreSQLに形態素解析データを格納
            dump_to_postgresql(ncode, ma_df)
        else:
            print("%sの形態素解析データは既にデータベースに存在しています" % ncode)


# 実行
main()