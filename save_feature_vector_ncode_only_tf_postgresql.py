# 作品の特徴ベクトルデータ（TF値のみ）をpostgreSQLに保存するプログラム

import pandas as pd
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

# 作品のTF値のデータベース名
ncode_tf_data_name = "tf_noun_data"

# 作品の特徴ベクトルを保存するデータベース名
feature_vector_ncode_tf_data_name = "feature_vector_ncode_%stf_noun_data_%snovels" % (tf_num, ncode_limit)

# 除外キーワード
except_list = ["ネット小説大賞九", "書籍化", "ネット小説大賞九感想", "HJ2021", "コミカライズ", "がうがうコン1", "ESN大賞３",
               "集英社小説大賞２", "OVL大賞7M", "集英社WEB小説大賞", "ESN大賞２", "キネノベ大賞２"]


# postgreSQLからメタデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


# TF値のデータがPostgreSQLに格納されているかの判定
def check_existed_feature_vector_ncode_tf_data(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE ncode='%s');" % (feature_vector_ncode_tf_data_name, ncode))
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# 除外キーワードをSQL文に入れる形にする関数
def get_except_list_sql(connection):
    # sql文に入れる用
    except_list_sql = "("
    # 作品コードのリストをsql文に書く形式に変更
    for keyword in except_list:
        except_list_sql += "'%s'," % keyword
    except_list_sql = except_list_sql[:-1] + ")"
    return except_list_sql


# postgreSQLから頻出上位x個のキーワードを取得
def get_keyword_ranking(connection, except_list_sql):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword, COUNT(keyword) FROM keyword_data WHERE keyword NOT IN %s GROUP BY keyword ORDER BY COUNT(keyword) DESC LIMIT %s" % (except_list_sql, keyword_limit),
        con=connection)
    return df


# postgreSQLからキーワード毎にTF値データを取得
def get_keyword_tf_data(connection, keyword):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE keyword='%s' ORDER BY tf DESC LIMIT %s;" % (keyword_tf_data_name, keyword, tf_num),
        con=connection)
    return df


# postgreSQLから作品毎にTF値データを取得
def get_ncode_tf_data(connection, ncode):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE ncode='%s' ORDER BY tf DESC LIMIT %s;" % (ncode_tf_data_name, ncode, tf_num),
        con=connection)
    return df


# 特徴ベクトルデータをpostgreSQLに格納する関数
def save_feature_vector_postgresql(connection, ncode, feature_vector):
    # print("作品コード「%s」の特徴ベクトルをデータベースに保存します" % ncode)
    cur = connection.cursor()
    # データを格納
    cur.execute("INSERT INTO %s (ncode, feature_vector) VALUES ('%s', ARRAY%s);" % (feature_vector_ncode_tf_data_name, ncode, feature_vector))
    # curを閉じる
    cur.close()
    # コミットする
    connection.commit()
    # print("データベースに保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # 除外リストのSQL文用の形を取得
    except_list_sql = get_except_list_sql(connection)
    # postgreSQLから頻出上位キーワードの取得
    keyword_ranking_df = get_keyword_ranking(connection, except_list_sql)
    # TF値上位のトークンの集合を格納するリスト
    all_token_list = []
    print("特徴ベクトルの次元となる各トークンのリストを取得します")
    # キーワードリストを順々に読み込む
    for keyword in keyword_ranking_df["keyword"]:
        # postgreSQLから上位のTF値を持つトークンのデータを取得
        tf_df = get_keyword_tf_data(connection, keyword)
        # トークン名のみのDataFrameを取得
        token_df = tf_df["token"]
        # リストに変換（トークン名のリストを取得）
        token_list = token_df.values.tolist()
        # トークン集合に追加
        for token in token_list:
            all_token_list.append(token)
    print("トークンの重複込の総数は%sでした" % len(all_token_list))
    # トークンの重複を削除
    all_token_list = sorted(set(all_token_list), key=all_token_list.index)
    print("トークンの総数は%sでした" % len(all_token_list))
    print(all_token_list)
    # postgreSQLからメタデータを取得
    metadata_df = get_postgresql_data(connection)
    # 作品コードデータを順々に読み込む
    for ncode in tqdm(metadata_df["ncode"]):
        # 既に特徴ベクトルデータがpostgreSQLに保存されていれば
        if check_existed_feature_vector_ncode_tf_data(connection, ncode)[0]:
            print("作品コード「%s」の特徴ベクトルデータは既にpostgreSQLに存在しています" % ncode)
        else:
            # postgreSQLから上位のTF値を持つトークンのデータを取得
            tf_df = get_ncode_tf_data(connection, ncode)
            # トークン名とTF値のリストを取得
            token_list = tf_df["token"].values.tolist()
            tf_list = tf_df["tf"].values.tolist()
            # トークン名をキー、TF値を値とした辞書を作成
            tf_dict = dict(zip(token_list, tf_list))
            # print(tf_dict)
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
            save_feature_vector_postgresql(connection, ncode, feature_vector)


# 実行
if __name__ == '__main__':
    main()