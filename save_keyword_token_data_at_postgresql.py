# IDF値を計算するためのキーワード毎の形態素データをPostgreSQLに格納するプログラム

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
ncode_limit = 50


# 形態素解析データのデータベース名
ma_data_name = "ma_data_not_stop_words"

# キーワードの形態素解析データを保存するデータベース名
keyword_token_data_name = "keyword_noun_data_%snovels" % ncode_limit

# 除外キーワード
except_list = ["ネット小説大賞九", "書籍化", "ネット小説大賞九感想", "HJ2021", "コミカライズ", "がうがうコン1", "ESN大賞３",
               "集英社小説大賞２", "OVL大賞7M", "集英社WEB小説大賞", "ESN大賞２", "キネノベ大賞２"]
except_list2 = "('ネット小説大賞九', '書籍化', 'ネット小説大賞九感想', 'HJ2021', 'コミカライズ', 'がうがうコン1', 'ESN大賞３', '集英社小説大賞２', 'OVL大賞7M', '集英社WEB小説大賞', 'ESN大賞２', 'キネノベ大賞２')"

# 形態素解析データがPostgreSQLに格納されているかの判定
def check_existed_token_data(connection, keyword):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE keyword='%s');" % (keyword_token_data_name, keyword))
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


# キーワードから作品コードのリストを取得
def get_ncode_list(connection, keyword):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword_data.ncode, metadata.global_point FROM keyword_data INNER JOIN metadata ON keyword_data.ncode=metadata.ncode WHERE keyword_data.keyword='%s' ORDER BY metadata.global_point DESC LIMIT %s;" % (keyword, ncode_limit),
        con=connection)
    return df


# キーワード集合内の各作品の形態素解析データを統合する
def marge_ma_data(connection, keyword, ncode_list, type):
    # 形態素解析データを入れるDataFrameを作成
    keyword_ma_df = pd.DataFrame()
    # 作品コードのリストを順々に読み込む
    for ncode in tqdm(ncode_list):
        # print("作品コード%sの作品の形態素解析データを読み込みます" % ncode)
        # 作品の形態素解析データを取得（作品コード、トークンの基本形のみ）
        ncode_ma_df = get_ma_data(connection, ncode, type)[["ncode", "token_lemma"]]
        # トークンの基本形について重複を削除
        ncode_ma_df = ncode_ma_df.drop_duplicates(["token_lemma"])
        # キーワードの形態素解析データに統合
        keyword_ma_df = keyword_ma_df.append(ncode_ma_df)
    # トークンの基本形について重複を削除
    keyword_ma_df = keyword_ma_df.drop_duplicates(["token_lemma"])
    # 作品コードの列を削除
    keyword_ma_df = keyword_ma_df.drop(columns="ncode")
    # インデックスを振り直す
    keyword_ma_df = keyword_ma_df.reset_index(drop=True)
    # トークン数と同等の長さで全要素を同じキーワードで埋めたリストの生成
    keyword_list = [keyword for i in range(len(keyword_ma_df))]
    # キーワードのみのDataFrameの列を作成
    keyword_df = pd.DataFrame({"keyword":keyword_list})
    print(keyword_df)
    # キーワードdfとトークンdfの結合
    result_df = keyword_df.join(keyword_ma_df)
    print(result_df)
    # 結果を出力
    return result_df


# postgreSQLから作品コードに対応する形態素解析データを取得
def get_ma_data(connection, ncode, type):
    # print("%sに対応する形態素解析データを取得します" % ncode)
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


# データベースに格納する関数
def save_postgresql(data_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    data_df.to_sql(keyword_token_data_name, con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからキーワードリストの取得（頻出上位xキーワード）
    keyword_df = get_keyword_ranking(connection)
    # キーワードリストを順々に読み込む
    for keyword in keyword_df["keyword"]:
        print("キーワード「%s」のデータを取得します" % keyword)
        # 既にデータがpostgreSQLに保存されていれば
        if check_existed_token_data(connection, keyword)[0]:
            print("キーワード「%s」のデータは既にpostgreSQLに存在しています" % keyword)
        else:
            # キーワードに対応する作品コードリストの取得（総合評価ポイント降順で上位x作品）
            ncode_df = get_ncode_list(connection, keyword)
            # キーワード集合内の各作品の形態素解析データを統合する
            keyword_ma_df = marge_ma_data(connection, keyword, ncode_df["ncode"], type=5)
            # トークンデータをpostgreSQLに保存
            save_postgresql(keyword_ma_df)


# 実行
if __name__ == '__main__':
    main()