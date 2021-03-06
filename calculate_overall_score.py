# キーワードを複数選択し、そこから特徴ベクトル同士の類似度計算を行い総合スコアを計算するプログラム
# 作品毎に総合スコアの計算後、総合スコア降順にソートし提示する

import pandas as pd
import numpy as np
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

# キーワードの特徴ベクトルを保存するデータベース名
feature_vector_tf_data_name = "feature_vector_%stf_noun_data_%snovels" % (tf_num, ncode_limit)

# 作品の特徴ベクトルを保存するデータベース名
feature_vector_ncode_tf_data_name = "feature_vector_ncode_%stf_noun_data_%snovels" % (tf_num, ncode_limit)

# 除外キーワード
except_list = ["ネット小説大賞九", "書籍化", "ネット小説大賞九感想", "HJ2021", "コミカライズ", "がうがうコン1", "ESN大賞３",
               "集英社小説大賞２", "OVL大賞7M", "集英社WEB小説大賞", "ESN大賞２", "キネノベ大賞２"]

# 入力パラメータのディレクトリ
parameta_dir = "parameta/"

# 推薦結果のディレクトリ
result_dir = "result/"

# 選択キーワードのテキストファイル
select_keyword_text = parameta_dir + "parameta1.txt"


# テキストファイルから選択キーワードの取得
def get_select_keyword():
    # キーワードのリスト
    keyword_list = []
    # ファイルを読み込む
    with open(select_keyword_text, encoding="shift_jis") as f:
        lines = f.readlines()
    # 行リストを1行ずつ読み込んでキーワードと重み付け値のリストに追加
    for line in lines:
        # 改行文字を空白に置換
        line = line.replace("\n", "")
        # 分割
        keyword, weighting = line.split("：")
        keyword_list.append(keyword)
    return keyword_list


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
        sql="SELECT keyword, COUNT(keyword) FROM keyword_data WHERE keyword NOT IN %s GROUP BY keyword ORDER BY COUNT(keyword) DESC LIMIT %s;" % (except_list_sql, keyword_limit),
        con=connection)
    return df


# postgreSQLから作品の特徴ベクトルのデータを取得
def get_feature_vector_ncode_data(connection, ncode_list):
    # sql文に入れる用
    ncode_list_sql = "("
    # 作品コードのリストをsql文に書く形式に変更
    for ncode in ncode_list:
        ncode_list_sql += "'%s'," % ncode
    ncode_list_sql = ncode_list_sql[:-1] + ")"
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT %s.ncode, %s.feature_vector FROM %s INNER JOIN metadata ON %s.ncode=metadata.ncode WHERE %s.ncode IN %s ORDER BY metadata.global_point;" % (feature_vector_ncode_tf_data_name, feature_vector_ncode_tf_data_name, feature_vector_ncode_tf_data_name, feature_vector_ncode_tf_data_name, feature_vector_ncode_tf_data_name, ncode_list_sql),
        con=connection)
    return df


# postgreSQLからキーワードの特徴ベクトルのデータを取得
def get_feature_vector_selected_keyword_data(connection, keyword_list):
    # sql文に入れる用
    keyword_list_sql = "("
    # 作品コードのリストをsql文に書く形式に変更
    for keyword in keyword_list:
        keyword_list_sql += "'%s'," % keyword
    keyword_list_sql = keyword_list_sql[:-1] + ")"
    # DataFrameでロード
    df = pd.read_sql(
        sql="SELECT * FROM %s WHERE keyword IN %s;" % (feature_vector_tf_data_name, keyword_list_sql),
        con=connection)
    return df


# postgreSQLからメタデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


# コサイン類似度の計算
def calculate_cosine_similarity(ncode_feature_vector, keyword_feature_vector):
    result = np.dot(ncode_feature_vector, keyword_feature_vector) / (np.linalg.norm(ncode_feature_vector) * np.linalg.norm(keyword_feature_vector))
    return result


# 総合スコアの計算
def calculate_overall_score(cosine_similarity_list):
    # 結果の値を格納する変数
    result = 0
    # 重みをかけて足す（現在重み値考慮せず）
    for cosine_similarity in cosine_similarity_list:
        result += cosine_similarity
    # 総数で割って100でかける
    result = (result / len(cosine_similarity_list)) * 100
    return result


# 推薦小説のデータをPostgreSQLから取得
def get_recommended_novel_data(connection, ncode_list):
    # sql文に入れる用
    ncode_list_sql = "("
    # 作品コードのリストをsql文に書く形式に変更
    for ncode, overall_score in ncode_list:
        ncode_list_sql += "'%s'," % ncode
    ncode_list_sql = ncode_list_sql[:-1] + ")"
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(
        sql="SELECT ncode, title, writer, story, keyword FROM metadata WHERE ncode IN %s;" % ncode_list_sql,
        con=connection)
    return df


# 推薦結果をテキストファイルに出力
def export_text_file(recommend_df, keyword_list, cosine_similarity_dict):
    # リスト化（作品名、タイトル、作者名、あらすじ、キーワード）
    recommend_data = recommend_df.values.tolist()
    # 出力するファイル名
    filename = ""
    # ファイル名を作成
    for keyword in keyword_list:
        filename += "【%s】" % keyword
    # 順位
    index = 0
    # ファイルを上書きモードで作成
    f = open(result_dir + "%s.txt" % filename, 'w')
    # 1作品ずつ書き込み
    for ncode, overall_score, title, writer, story, keyword in recommend_data:
        # 順位を1ずつ増やす
        index += 1
        # コサイン類似度のリストを取得
        cosine_similarity_list = cosine_similarity_dict[ncode]
        # テキスト表示用
        keyword_score_text = ""
        # 各キーワードとのコサイン類似度を取得
        for keyword, cosine_similarity in zip(keyword_list, cosine_similarity_list):
            keyword_score_text += "【%s：%s点】" % (keyword, cosine_similarity * 100)
        # 作品のURL
        url = "https://ncode.syosetu.com/%s/" % ncode
        # ファイルに書き込み
        f.write("【%s位（総合スコア：%s点）】\n個別スコア：%s\n作品コード：%s\n作品名：%s\n作者名：%s\nURL：%s\nあらすじ：\n%s\n\n" % (index, overall_score, keyword_score_text, ncode, title, writer, url, story))
    f.close()


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # postgreSQLからメタデータを取得
    metadata_df = get_postgresql_data(connection)
    # 作品コードのみのリストに変換
    ncode_list = metadata_df["ncode"].values.tolist()
    # 特徴ベクトルのデータを取得
    feature_vector_ncode_df = get_feature_vector_ncode_data(connection, ncode_list)
    # リストに変換
    feature_vector_ncode_list = feature_vector_ncode_df[["ncode", "feature_vector"]].values.tolist()
    # 選択キーワードと重み付け値の取得
    select_keyword_list = get_select_keyword()
    # 選出キーワードの特徴ベクトルデータを取得
    feature_vector_keyword_df = get_feature_vector_selected_keyword_data(connection, select_keyword_list)
    # キーワードと特徴ベクトルのリストに変換
    keyword_list = feature_vector_keyword_df["keyword"].values.tolist()
    feature_vector_list = feature_vector_keyword_df["feature_vector"].values.tolist()
    # 作品コードとコサイン類似度の組を格納する辞書
    ncode_cosine_similarity_dict = {}
    # キーワードをキー、特徴ベクトルを値とした辞書を作成
    feature_vector_keyword_dict = dict(zip(keyword_list, feature_vector_list))
    # 作品コードと総合スコアを格納するリスト
    overall_score_list = []
    # 作品コードのリストを1つずつ読み込む
    for ncode, feature_vector in tqdm(feature_vector_ncode_list):
        # 各キーワードの特徴ベクトルとのコサイン類似度を格納するリスト
        cosine_similarity_list = []
        # 選出キーワードを1つずつ読み込む
        for keyword in keyword_list:
            # Demicalになっているのでfloat型に変換
            ncode_feature_vector = [float(element) for element in feature_vector]
            keyword_feature_vector = [float(element) for element in feature_vector_keyword_dict[keyword]]
            # 各キーワードの特徴ベクトルとコサイン類似度の計算
            cosine_similarity = calculate_cosine_similarity(ncode_feature_vector, keyword_feature_vector)
            # print("作品コード「%s」のキーワード「%s」とのコサイン類似度は%sでした" % (ncode, keyword, cosine_similarity))
            # リストに追加
            cosine_similarity_list.append(cosine_similarity)
        # 辞書に作品コードをキー、コサイン類似度のリストを値として追加
        ncode_cosine_similarity_dict[ncode] = cosine_similarity_list
        # 総合スコアの計算
        overall_score = calculate_overall_score(cosine_similarity_list)
        # リストに追加
        overall_score_list.append([ncode, overall_score])
        # print("作品コード「%s」の総合スコアは%sでした" % (ncode, overall_score))
    # 総合スコアのリストをDataFrameに変換
    overall_score_df = pd.DataFrame(overall_score_list, columns=["ncode", "overall_score"])
    # 総合スコアで降順にソート
    result_df = overall_score_df.sort_values(by="overall_score", ascending=False)
    # インデックスの振り直し
    result_df = result_df.reset_index()
    # リスト化
    result_list = result_df[["ncode", "overall_score"]].values.tolist()
    # データの取得
    recommended_novel_df = get_recommended_novel_data(connection, result_list)
    recommended_data = result_df[["ncode", "overall_score"]].merge(recommended_novel_df)
    print(recommended_data.head(100))
    print(recommended_data.head(100).values.tolist())
    export_text_file(recommended_data.head(100), select_keyword_list, ncode_cosine_similarity_dict)


# 実行
if __name__ == '__main__':
    main()