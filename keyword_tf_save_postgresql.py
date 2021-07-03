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

# 形態素解析データのデータベース名
ma_data_name = "ma_data_not_stop_words"

# キーワードの形態素解析データが保存されているデータベース名
keyword_token_data_name = "keyword_noun_data_%snovels" % ncode_limit

# 作品のTF値のデータベース名
ncode_tf_data_name = "tf_noun_data"

# キーワードのTF値のデータベース名
keyword_tf_data_name = "keyword_tf_noun_data_%snovels" % ncode_limit

# 除外キーワード
except_list = ["ネット小説大賞九", "書籍化", "ネット小説大賞九感想", "HJ2021", "コミカライズ", "がうがうコン1", "ESN大賞３",
               "集英社小説大賞２", "OVL大賞7M", "集英社WEB小説大賞", "ESN大賞２", "キネノベ大賞２"]


# TF値のデータがPostgreSQLに格納されているかの判定
def check_existed_keyword_tf_data(connection, keyword):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE keyword='%s');" % (keyword_tf_data_name, keyword))
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


# キーワードから作品コードのリストを取得
def get_ncode_list(connection, keyword):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword_data.ncode, metadata.global_point FROM keyword_data INNER JOIN metadata ON keyword_data.ncode=metadata.ncode WHERE keyword_data.keyword='%s' ORDER BY metadata.global_point DESC LIMIT %s;" % (keyword, ncode_limit),
        con=connection)
    return df


# postgreSQLからキーワードのトークンデータを取得
def get_keyword_token_data(connection, keyword):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE keyword='%s';" % (keyword_token_data_name, keyword),
        con=connection)
    return df


# postgreSQLから作品のTF値データを取得
def get_ncode_tf_data(connection, ncode_list):
    # sql文に入れる用
    ncode_list_sql = "("
    # 作品コードのリストをsql文に書く形式に変更
    for ncode in ncode_list:
        ncode_list_sql += "'%s'," % ncode
    ncode_list_sql = ncode_list_sql[:-1] + ")"
    print(ncode_list_sql)
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE ncode IN %s;" % (ncode_tf_data_name, ncode_list_sql),
        con=connection)
    return df


# キーワード集合内の各作品のTF値データを統合する
def marge_tf_data(connection, keyword, ncode_list):
    # TF値のデータを入れるリスト
    keyword_tf_list = []
    # postgreSQLから作品リスト全てのTF値データを取得
    ncode_tf_df = get_ncode_tf_data(connection, ncode_list)
    # DataFrameをリスト化（トークン、TF値のみ）
    ncode_tf_list = ncode_tf_df[["token", "tf"]].values.tolist()
    # postgreSQLからキーワードのトークンデータを取得
    keyword_token_df = get_keyword_token_data(connection, keyword)
    # キーワードのトークンデータを1個ずつ読み込む
    for token in tqdm(keyword_token_df["token_lemma"]):
        # 各作品の任意のトークンについてのTF値の集合を作成
        tf_list = [tf for tok, tf in ncode_tf_list if tok == token]
        # キーワードのTF値を計算（作品毎のTF値を全て足してから作品数で割る）
        keyword_token_tf = sum(tf_list)/ncode_limit
        # トークンのTF値の組をリストに追加
        keyword_tf_list.extend([[keyword, token, keyword_token_tf]])
    # TF値のリストをDataFrameに変換
    keyword_tf_df = pd.DataFrame(keyword_tf_list, columns=["keyword", "token", "tf"])
    # TF値で降順にソート
    result_df = keyword_tf_df.sort_values(by="tf", ascending=False)
    print(result_df)
    return result_df


# データベースに格納する関数
def save_postgresql(data_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    data_df.to_sql(keyword_tf_data_name, con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # 除外リストのSQL文用の形を取得
    except_list_sql = get_except_list_sql(connection)
    # postgreSQLから頻出上位キーワードの取得
    keyword_ranking_df = get_keyword_ranking(connection, except_list_sql)
    # キーワードリストを順々に読み込む
    for keyword in keyword_ranking_df["keyword"]:
        print("キーワード「%s」のTF値データを取得します" % keyword)
        # 既にデータがpostgreSQLに保存されていれば
        if check_existed_keyword_tf_data(connection, keyword)[0]:
            print("キーワード「%s」のデータは既にpostgreSQLに存在しています" % keyword)
        else:
            # キーワードに対応する作品コードリストの取得（総合評価ポイント降順で上位x作品）
            ncode_df = get_ncode_list(connection, keyword)
            # キーワード集合内の各作品の形態素解析データを統合する
            keyword_tf_df = marge_tf_data(connection, keyword, ncode_df["ncode"])
            # トークンデータをpostgreSQLに保存
            save_postgresql(keyword_tf_df)


# 実行
if __name__ == '__main__':
    main()