# 形態素解析のテスト

import spacy
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

# エンティティタイプ用
ent_iob_list = []

# ストップリスト用
stop_list = []
not_stop_list = []

# ネガポジ用
sentiment_list = []

# 語彙外か
is_oob_list = []

# DataFrameの列名
# [ncode, line_num, tok.i, tok.text, tok.orth_, tok.lemma_, tok.pos_, tok.tag_, tok.head.i, tok.dep_, tok.norm_, tok.ent_iob_, tok.ent_type_]
df_columns = ["ncode", "line_num", "tok_index", "tok_text", "tok_orth", "tok_lemma", "tok_pos", "tok_tag", "tok_head_index", "tok_dep", "tok_norm", "tok_ent_iob", "tok_ent_type"]


# postgreSQLからデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode FROM metadata WHERE ncode IN (SELECT ncode FROM text_data) ORDER BY global_point DESC;", con=connection)
    return df


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
    # 1行ごとに形態素解析を行う
    for line in tqdm(lines):
        doc = nlp(line)
        # 各トークンの情報を取得
        for tok in doc:
            # トークン情報をまとめたリスト
            token_info = [ncode, line_num, tok.i, tok.text, tok.orth_, tok.lemma_, tok.pos_, tok.tag_, tok.head.i, tok.dep_, tok.norm_, tok.ent_iob_, tok.ent_type_]
            print(
               "ncode:" + ncode + "\n"
               "line_num（行番号）\u3000" + str(line_num) + "\n"
               "tok.i（トークン番号）\u3000" + str(tok.i) + "\n" 
               "tok.text（逐語的なテキストコンテンツ）\u3000" + tok.text + "\n"
               "tok.lemma_（トークンの基本形）\u3000" + tok.lemma_ + "\n"
               "tok.pos_（粗い品詞）\u3000" + tok.pos_ + "\n"
               "tok.tag_（きめの細かい品詞）\u3000" + tok.tag_ + "\n"
               "tok.head.i（トークンの構造上の親のトークン番号）\u3000" + str(tok.head.i) + "\n"
               "tok.dep_（構文依存関係）\u3000" + tok.dep_ + "\n"
               "tok.norm_（トークンテキストの正規化された形式）\u3000" + tok.norm_ + "\n"
               "ent_iob_（名前付きエンティティタイプのIOBコード\u3000" + tok.ent_iob_ + "\n"
               "ent_type_（名前付きエンティティタイプ）\u3000" + tok.ent_type_ + "\n"
               "is_stop（ストップリストかどうか）\u3000" + str(tok.is_stop) + "\n"
               "sentiment（ポジティブまたはネガティブを示すスカラー値）\u3000" + str(tok.sentiment) + "\n"
               "is_oov（語彙外かどうか）\u3000" + str(tok.is_oov) + "\n"
            )

            # エンティティタイプがO以外のものを調査
            if tok.ent_iob_ != "O":
                ent_iob_list.append([tok.text, tok.ent_iob_, tok.ent_type_])
            else:
                pass

            # ストップリストかそうでないかの調査
            if tok.is_stop:
                stop_list.append(tok.text)
            else:
                not_stop_list.append(tok.text)

            # ネガポジ
            if tok.sentiment != 0.0:
                sentiment_list.append([tok.text, tok.sentiment])
            else:
                pass

            # DataFrameのline_num行目に追加
            ma_df.loc[line_num] = token_info
        # 行数カウントを1増やす
        line_num += 1

        print(ent_iob_list)
        print(stop_list)
        print(not_stop_list)
        print(sentiment_list)

    return ma_df


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからメタデータの取得
    df = get_postgresql_data(connection)
    # データリストからncodeを順々に読み込んでいく
    for ncode in df["ncode"]:
        print("%sの形態素解析を開始します" % ncode)
        ma_df = morphological_analysis(connection, ncode)
        print(ma_df)


# 実行
if __name__ == '__main__':
    main()