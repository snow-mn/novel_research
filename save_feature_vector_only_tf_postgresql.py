# キーワード集合内の単語のTF値をPostgreSQLに格納するプログラム

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

# キーワード集合、キーワード毎の作品集合の上限
keyword_limit = 100
ncode_limit = 20

# 特徴ベクトルに反映するTF値を上位何個までにするか
tf_num = 100

# キーワードのTF値のデータベース名
keyword_tf_data_name = "keyword_tf_noun_data_%snovels" % ncode_limit

# 特徴ベクトルの各次元に対応するトークンを保存するデータベース名
feature_vector_token_data_name = "feature_vector_token_data_%snovels" % ncode_limit

# キーワードの特徴ベクトルを保存するデータベース名
feature_vector_tf_data_name = "feature_vector_%stf_noun_data_%snovels" % (tf_num, ncode_limit)

# 作品の特徴ベクトルを保存するデータベース名
feature_vector_ncode_tf_data_name = "feature_vector_ncode_%stf_noun_data_%snovels" % (tf_num, ncode_limit)

# 除外キーワード
except_list = ["ネット小説大賞九", "書籍化", "ネット小説大賞九感想", "HJ2021", "コミカライズ", "がうがうコン1", "ESN大賞３",
               "集英社小説大賞２", "OVL大賞7M", "集英社WEB小説大賞", "ESN大賞２", "キネノベ大賞２"]
except_list2 = "('ネット小説大賞九', '書籍化', 'ネット小説大賞九感想', 'HJ2021', 'コミカライズ', 'がうがうコン1', 'ESN大賞３', '集英社小説大賞２', 'OVL大賞7M', '集英社WEB小説大賞', 'ESN大賞２', 'キネノベ大賞２')"


# TF値のデータがPostgreSQLに格納されているかの判定
def check_existed_feature_vector_tf_data(connection, keyword):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE keyword='%s');" % (feature_vector_tf_data_name, keyword))
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# postgreSQLから頻出上位x個のキーワードを取得
def get_keyword_ranking(connection):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword, COUNT(keyword) FROM keyword_data WHERE keyword NOT IN %s GROUP BY keyword ORDER BY COUNT(keyword) DESC LIMIT %s" % (except_list2, keyword_limit),
        con=connection)
    return df


# postgreSQLからキーワード毎にTF値データを取得
def get_keyword_td_data(connection, keyword):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE keyword='%s' ORDER BY tf DESC LIMIT %s;" % (keyword_tf_data_name, keyword, tf_num),
        con=connection)
    return df


# 特徴ベクトルデータをpostgreSQLに格納する関数
def save_feature_vector_postgresql(connection, keyword, feature_vector):
    print("キーワード「%s」の特徴ベクトルをデータベースに保存します" % keyword)
    cur = connection.cursor()
    # データを格納
    cur.execute("INSERT INTO %s (keyword, feature_vector) VALUES ('%s', ARRAY%s);" % (feature_vector_tf_data_name, keyword, feature_vector))
    # curを閉じる
    cur.close()
    # コミットする
    connection.commit()
    print("データベースに保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからキーワードリストの取得（頻出上位xキーワード）
    keyword_df = get_keyword_ranking(connection)
    # TF値上位のトークンの集合を格納するリスト
    all_token_list = []
    # キーワードリストを順々に読み込む
    for keyword in keyword_df["keyword"]:
        print("キーワード「%s」のTF値データを取得します" % keyword)
        # postgreSQLから上位のTF値を持つトークンのデータを取得
        tf_df = get_keyword_td_data(connection, keyword)
        # トークン名のみのDataFrameを取得
        token_df = tf_df["token"]
        # リストに変換（トークン名のリストを取得）
        token_list = token_df.values.tolist()
        print(token_list)
        # トークン集合に追加
        for token in token_list:
            all_token_list.append(token)
        print(all_token_list)
    print("トークンの重複込の総数は%sでした" % len(all_token_list))
    # トークンの重複を削除
    all_token_list = sorted(set(all_token_list), key=all_token_list.index)
    print("トークンの総数は%sでした" % len(all_token_list))
    print(all_token_list)
    # キーワードリストを順々に読み込む
    for keyword in keyword_df["keyword"]:
        # 既に特徴ベクトルデータがpostgreSQLに保存されていれば
        if check_existed_feature_vector_tf_data(connection, keyword)[0]:
            print("キーワード「%s」の特徴ベクトルデータは既にpostgreSQLに存在しています" % keyword)
        else:
            # postgreSQLから上位のTF値を持つトークンのデータを取得
            tf_df = get_keyword_td_data(connection, keyword)
            # トークン名とTF値のリストを取得
            token_list = tf_df["token"].values.tolist()
            tf_list = tf_df["tf"].values.tolist()
            # トークン名をキー、TF値を値とした辞書を作成
            tf_dict = dict(zip(token_list, tf_list))
            print(tf_dict)
            # 特徴ベクトルを入れるリスト
            feature_vector = []
            # 特徴ベクトル用のトークンリストを1個ずつ読み込む
            for token in all_token_list:
                # TF値が存在しているならば
                if token in tf_dict.keys():
                    # 辞書からTF値を取得
                    tf = tf_dict[token]
                # 存在してないならば
                else:
                    # TF値は0
                    tf = 0
                # 特徴ベクトルに追加
                feature_vector.append(tf)
            # postgreSQLに保存
            save_feature_vector_postgresql(connection, keyword, feature_vector)


# 実行
if __name__ == '__main__':
    main()