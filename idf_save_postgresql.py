# IDF値をPostgreSQLに格納するプログラム

import pandas as pd
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

# IDF値を格納するデータベース名（NOUN）
# キーワード数20
idf_data_name = "idf_noun_data_20"

# キーワード集合、キーワード毎の作品集合の上限
keyword_limit = 20
ncode_limit = 20


# IDF値データがPostgreSQLに格納されているかの判定
def check_existed_idf(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE ncode='%s');" % (idf_data_name, ncode))
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# 形態素解析データがPostgreSQLに格納されているかの判定
def check_existed_ma(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM %s WHERE ncode='%s');" % (ma_data_name, ncode))
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# postgreSQLから頻出上位x個のキーワードを取得
def get_keyword_ranking(connection):
    # DataFrameでロード（頻出上位降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword, COUNT(keyword) FROM keyword_data GROUP BY keyword ORDER BY COUNT(keyword) DESC LIMIT %s" % keyword_limit,
        con=connection)
    return df


# キーワードから作品コードのリストを取得
def get_ncode_list(connection, keyword):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT keyword_data.ncode, metadata.global_point FROM keyword_data INNER JOIN metadata ON keyword_data.ncode=metadata.ncode WHERE keyword_data.keyword='%s' ORDER BY metadata.global_point DESC LIMIT %s;" % (keyword, ncode_limit),
        con=connection)
    return df


# postgreSQLから既にテキストデータが存在する作品コードのデータを取得
def get_ncode_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;",
        con=connection)
    return df


# キーワード集合内の各作品の形態素解析データを統合する
def marge_ma_data(connection, ncode_list, type):
    # 形態素解析データを入れるDataFrameを作成
    keyword_ma_df = pd.DataFrame()
    # 作品コードのリストを順々に読み込む
    for ncode in tqdm(ncode_list):
        print("作品コード%sの作品の形態素解析データを読み込みます" % ncode)
        # 作品の形態素解析データを取得（作品コード、トークンの基本形のみ）
        ncode_ma_df = get_ma_data(connection, ncode, type)[["ncode", "token_lemma"]]
        # トークンの基本形について重複を削除
        ncode_ma_df = ncode_ma_df.drop_duplicates(["token_lemma"])
        # キーワードの形態素解析データに統合
        keyword_ma_df = keyword_ma_df.append(ncode_ma_df)
    # トークンの基本形について重複を削除
    keyword_ma_df = keyword_ma_df.drop_duplicates(["token_lemma"])
    print(keyword_ma_df)
    # 結果を出力
    return keyword_ma_df



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


# IDFの計算
def calculate_idf(connection, ncode):
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
    # 出力
    return data_df


# データベースに格納する関数
def save_postgresql(data_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    data_df.to_sql(idf_data_name, con=engine, if_exists='append', index=False)
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
        # キーワードに対応する作品コードリストの取得（総合評価ポイント降順で上位x作品）
        ncode_df = get_ncode_list(connection, keyword)
        # キーワード集合内の各作品の形態素解析データを統合する
        keyword_ma_df = marge_ma_data(connection, ncode_df["ncode"], type=5)


# 実行
if __name__ == '__main__':
    main()