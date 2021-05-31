# 形態素解析を行い、データベースに形態素解析情報を格納するプログラム

import spacy
import os
import pandas as pd
import tqdm
import psycopg2
from sqlalchemy import create_engine
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
# [ncode, line_num, tok.i, tok.text, tok.orth_, tok.lemma_, tok.pos_, tok.tag_, tok.head.i, tok.dep_, tok.norm_, tok.ent_.iob_, tok.ent_.type_]
df_columns = ["ncode", "line_num", "tok_index", "tok_text", "tok_orth", "tok_lemma", "tok_tag", "tok_head_index", "tok_dep", "tok_norm", "tok_ent_iob", "tok_ent_type"]


# 形態素解析
def morphological_analysis(ncode, lines):
    # ginzaの準備
    nlp = spacy.load("ja_ginza")
    # dataframeの作成
    ma_df = pd.DataFrame(columns=df_columns)
    # 何行目かカウントする変数
    line_num = 0
    # 1行ごとに形態素解析を行う
    for line in tqdm.tqdm(lines):
        doc = nlp(line)
        # 各トークンの情報を取得
        for tok in doc:
            print(
                tok.i,  # 親ドキュメント内のトークンのインデックス。
                tok.text,  # 逐語的なテキストコンテンツ

                # 逐語的なテキストコンテンツ（Token.textと同じ）
                # 主に他の属性との一貫性のために存在します。
                tok.orth_,

                tok.lemma_,  # 語尾変化のない接尾辞のない、トークンの基本形式
                tok.pos_,  # 品詞（英語の大文字）
                tok.tag_,  # きめの細かい品詞
                tok.head.i,  # headはこのトークンの構文上の親、または「ガバナー」
                tok.dep_,  # 構文従属関係

                # トークンの基準、つまりトークンテキストの正規化された形式
                # 通常、言語のトークナイザー例外またはノルム例外で設定されます。
                tok.norm_,

                # 名前付きエンティティタグのIOBコード
                # 「B」はトークンがエンティティを開始することを意味し、「I」はそれがエンティティの内部にあることを意味し、
                # 「O」はそれがエンティティの外部にあることを意味し、「」はエンティティタグが設定されていないことを意味します。
                tok.ent_iob_,
                tok.ent_type_,  # 名前付きエンティティタイプ
            )
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
    print("データベースに接続開始します")
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))
    # PostgreSQLのテーブルにDataFrameを追加する
    ma_df.to_sql("metadata", con=engine, if_exists='append', index=False)
    print("取得トークン数  ", len(ma_df));
    print("データベースにデータを保存しました")


# メイン関数
def main_function():
    pass