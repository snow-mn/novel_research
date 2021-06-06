# 形態素解析を行い、データベースに形態素解析情報を格納するプログラム

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

# DataFrameの列名
df_columns = ["ncode", "line_index", "token_index", "token_text", "token_lemma", "token_pos", "token_tag",
              "token_head_index", "token_dep", "token_norm", "token_ent_iob", "token_ent_type",
              "token_is_stop", "token_is_oov", "token_vector", "token_sent"]


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
def morphological_analysis(connection, ncode):
    # PostgreSQLからDataFrameを取得
    df = pd.read_sql(sql="SELECT honbun FROM text_data WHERE ncode='%s';" % ncode, con=connection)
    # DataFrameから本文データを取り出す
    lines = df["honbun"][0]
    # ginzaの準備
    nlp = spacy.load("ja_ginza")
    # DataFrameの作成
    ma_df = pd.DataFrame(columns=df_columns)
    # 何行目かカウントする変数
    line_num = 0
    # 各要素を入れるリスト
    ncode_list = []
    # 行番号
    line_index = []
    # トークン番号
    token_index = []
    # 逐次的なテキストコンテンツ
    token_text = []
    # トークンの基本形
    token_lemma = []
    # 粗い品詞
    token_pos = []
    # きめの細かい品詞
    token_tag = []
    # トークンの構造上の親のトークン番号
    token_head_index = []
    # 構文依存関係
    token_dep = []
    # トークンのテキストの正規化された形式
    token_norm = []
    # 名前付きエンティティタイプのIOBコード
    token_ent_iob = []
    # 名前付きエンティティタイプ
    token_ent_type = []
    # ストップリストかどうか
    token_is_stop =[]
    # 語彙外（単語ベクトルがない）かどうか
    token_is_oov = []
    # 単語ベクトル
    token_vector = []
    # トークンが含まれるセンテンススパン
    token_sent = []

    # 1行ごとに形態素解析を行う
    for line in tqdm(lines):
        doc = nlp(line)
        # 各トークンの情報を取得
        for tok in doc:
            # 要素の追加
            ncode_list += [ncode]
            line_index += [line_num]
            token_index += [tok.i]
            token_text += [tok.text]
            token_lemma += [tok.lemma_]
            token_pos += [tok.pos_]
            token_tag += [tok.tag_]
            token_head_index += [tok.head.i]
            token_dep += [tok.dep_]
            token_norm += [tok.norm_]
            token_ent_iob += [tok.ent_iob_]
            token_ent_type += [tok.ent_type_]
            token_is_stop += [tok.is_stop]
            token_is_oov += [tok.is_oov]
            token_vector += [tok.vector.tolist()]
            token_sent += [str(tok.sent)]

        # 行数カウントを1増やす
        line_num += 1

    print("リストをDataFrameに変換しています")
    # リストをDataFrame化
    ma_df = pd.DataFrame(
        data={"ncode": ncode, "line_index": line_index, "token_index": token_index, "token_text": token_text,
              "token_lemma": token_lemma, "token_pos": token_pos, "token_tag": token_tag,
              "token_head_index": token_head_index, "token_dep": token_dep, "token_norm": token_norm,
              "token_ent_iob": token_ent_iob, "token_ent_type": token_ent_type, "token_is_stop": token_is_stop,
              "token_is_oov": token_is_oov, "token_vector": token_vector,  "token_sent": token_sent},
        columns=df_columns
    )
    print(ma_df)
    return ma_df


# データベースに格納する関数
def save_postgresql(ma_df):
    print("データをデータベースに格納します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    ma_df.to_sql("ma_data", con=engine, if_exists='append', index=False)
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