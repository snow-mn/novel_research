# 形態素解析を行い、データベースに形態素解析情報を格納するプログラム
# ストップワードを除外してデータベースに格納する
# tok.is_stop、PUNCT、SYMなど

import spacy
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import collections
from tqdm import tqdm

# データベースの接続情報
connection_config = {
    "host": "localhost",
    "port": "5432",
    "dbname": "narou_data",
    "user": "postgres",
    "password": "password"
}

# 形態素解析データのデータベース名
ma_data_name = "ma_data_not_stop_words"

# TF-IDF値を格納するデータベース名
tfidf_data_name = "tfidf_data"

# DataFrameの列名
df_columns = ["ncode", "line_index", "token_index", "token_text", "token_lemma", "token_pos", "token_tag",
              "token_dep", "token_norm", "token_ent_iob", "token_ent_type", "token_is_oov", "token_sent"]


# postgreSQLから既にテキストデータが存在する作品コードのデータを取得
def get_ncode_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;",
        con=connection)
    return df


# postgreSQLからキーワード出現回数データの取得
def get_keyword_count_data(connection):
    # DataFrameでロード(出現回数降順に10回以上のものを）
    df = pd.read_sql(
        sql="SELECT keyword, COUNT(keyword) FROM keyword_data GROUP BY keyword HAVING COUNT(*) >= 10 ORDER BY COUNT(keyword) DESC;",
        con=connection)
    return df


# postgreSQLからキーワードに対応する作品データを取得
# keyword_data自体テキストデータが存在する作品コードのデータのみなのでそこから絞る必要はない
def get_keyword_ncode_data(connection, keyword):
    print("%sに対応する作品データを取得します" % keyword)
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword_data.ncode FROM keyword_data INNER JOIN metadata ON keyword_data.ncode=metadata.ncode WHERE keyword_data.keyword='%s' ORDER BY metadata.global_point DESC;" % keyword,
        con=connection)
    return df


# postgreSQLから作品コードに対応する形態素解析データを取得
def get_ma_data(connection, ncode, type):
    print("%sに対応する形態素解析データを取得します" % ncode)
    # DataFrameでロード（総合評価ポイント降順にソート）
    if type == 0:
        # 全てのデータを取得
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    elif type == 1:
        # 名詞のみのデータを取得（NOUN）
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' AND token_pos='NOUN' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    elif type == 2:
        # 動詞のみのデータを取得（VERB）
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' AND token_pos='VERB' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    elif type == 3:
        # 形容詞のみのデータを取得（ADJ）
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' AND token_pos='ADJ' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    elif type == 4:
        # 副詞のみのデータを取得（ADV）
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' AND token_pos='ADV' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    elif type == 5:
        # NOUN（人名除外）
        df = pd.read_sql(
            sql="SELECT * FROM %s WHERE ncode='%s' AND token_pos='NOUN' AND token_ent_type != 'Person' AND token_tag !~ '.*人名.*' ORDER BY line_index ASC, token_index ASC;" % (ma_data_name, ncode),
            con=connection)
    # データを返す
    return df


# TFの計算
def calculate_tf(connection, ncode):
    # postgreSQLから作品コードに対応する形態素解析データを取得
    # 名詞のみのデータを取得
    ma_df = get_ma_data(connection, ncode, 5)
    # DataFrameの複製
    ma_df_copy = ma_df.copy()
    # 列数を取得
    df_len = len(ma_df)
    # トークンの基本形について重複を削除
    ma_df = ma_df.drop_duplicates(["token_lemma"])
    # 形態素解析データのDataframeをリスト化（token_lemmaの列のみ取得）
    ma_list = ma_df["token_lemma"].values.tolist()
    ma_list_copy = ma_df_copy["token_lemma"].values.tolist()
    # 形態素の重複回数をキーと値の辞書型で取得
    counter = collections.Counter(ma_list_copy)
    # TF値の辞書
    tf_dict = {}
    # 1行ずつデータを読み込む
    for lemma in ma_list:
        # TF値の算出
        tf = counter[lemma] / df_len
        # 辞書に追加
        tf_dict[lemma] = tf
    # 辞書の値で降順にソート
    tf_sorted_list = sorted(tf_dict.items(), key=lambda x:x[1], reverse=True)
    # print(tf_sorted_list)
    print("長さは%s行でした" % len(tf_sorted_list))
    # 形態素数と同等の長さで全要素を同じ作品コードで埋めたリストの生成
    ncode_list = [ncode for i in range(len(tf_sorted_list))]
    # 各リストをDataFrame化
    tf_df = pd.DataFrame(tf_sorted_list, columns=["token", "tf"])
    ncode_df = pd.DataFrame(ncode_list, columns=["ncode"])
    # DataFrame同士の結合
    data_df = ncode_df.join(tf_df)
    print(data_df)


# データベースに格納する関数
def save_postgresql(data_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    data_df.to_sql(tfidf_data_name, con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからキーワード出現回数データの取得
    keyword_count_df = get_keyword_count_data(connection)
    # キーワード出現回数データを1行ずつ読み込む
    for k_row in keyword_count_df.itertuples():
        # postgreSQLからキーワードに対応する作品コードのデータを取得
        ncode_df = get_keyword_ncode_data(connection, k_row[1])
        # キーワードに対応する作品コードを1行ずつ読み込む
        for n_row in ncode_df.itertuples():
            # TFの計算
            calculate_tf(connection, n_row[1])


# 実行
if __name__ == '__main__':
    main()