# 名詞句を抽出して出現回数をカウントし、エクセルファイルで出力するプログラム
# 同一キーワードの作品全てについて名詞句の出現回数を合計する

import spacy
import os
import pandas as pd
import numpy as np
import re
import tqdm

# パス
data_dir = "../novel_Data/"
text_dir = "../novel_Text/"
noun_chunks_count_data_dir = data_dir + "noun_chunks_count_data/"
noun_chunks_total_count_data_dir = data_dir + "noun_chunks_total_count_data/"


# データディレクトリがない場合は作成する関数
def make_count_dir():
    if not os.path.exists(data_dir + "noun_chunks_count_data"):
        os.mkdir(data_dir + "noun_chunks_count_data")
    else:
        pass
    if not os.path.exists(data_dir + "noun_chunks_total_count_data"):
        os.mkdir(data_dir + "noun_chunks_total_count_data")
    else:
        pass


# ディレクトリ一覧を取得
def load_dirs():
    # ディレクトリ内のディレクトリとファイルの一覧を取得
    file_dir_list = os.listdir(text_dir)
    # ディレクトリ名のみの一覧を取得
    dirs = [f for f in file_dir_list if os.path.isdir(os.path.join(text_dir, f))]
    # print(dirs)
    return dirs


# ファイル一覧を取得
def load_files(dir_name):
    dir_path = text_dir + dir_name
    # ディレクトリ内のディレクトリとファイルの一覧を取得
    file_dir_list = os.listdir(dir_path)
    # ファイル名のみの一覧を取得
    files = [f for f in file_dir_list if os.path.isfile(os.path.join(dir_path, f))]
    # print(files)
    return files


# 名詞句出現回数をexcelファイルに保存（作品個別用）
def save_xlsx(keyword, file_name, sorted_dict):
    # キーワードのディレクトリが存在していないなら作成
    if os.path.exists(noun_chunks_count_data_dir + keyword):
        pass
    else:
        os.mkdir(noun_chunks_total_count_data_dir + keyword)
    # ファイルパス
    xlsx_path = noun_chunks_count_data_dir + keyword + "/" + file_name.replace(".txt", "") + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_dict, columns=["noun_chunks", "count"])
    print(df)
    # エクセルファイルを作成・保存
    df.to_excel(xlsx_path, sheet_name="noun_chunks_count")


# 名詞句出現回数をexcelファイルに保存（全作品合計）
def save_xlsx_total(keyword, sorted_token_count):
    # ファイルパス
    xlsx_path = noun_chunks_total_count_data_dir + keyword + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_token_count, columns=["noun_chunks", "count"])
    print(df)
    # エクセルファイルを作成・保存
    df.to_excel(xlsx_path, sheet_name="noun_chunks_count")
    print(xlsx_path + "の保存が完了しました")


def noun_chunks_extract(dir_name, file_name, span_count):
    # ファイルパス
    file_path = text_dir + dir_name + "/" + file_name
    # モデルのロード
    nlp = spacy.load("ja_ginza")
    # テキストファイルの読み込み
    with open(file_path, mode="rt", encoding="utf-8") as f:
        read_data = f.readlines()
    # 1行毎に形態素解析
    print(file_path + "の形態素解析を開始します")
    for line in tqdm.tqdm(read_data):
        # 空白文字を取り除く
        line = re.sub(r"[\u3000\s「」]", "", line)
        # lineが空白でなければ形態素解析を行う
        if line != "":
            doc = nlp(line)
            for span in doc.noun_chunks:
                if str(span) in span_count:
                    span_count[str(span)] = span_count[str(span)] + 1
                else:
                    span_count[str(span)] = 1


# メイン関数
def main_function():
    dirs = load_dirs()
    for dir in dirs:
        # 既にエクセルデータが存在するか
        if not os.path.exists(noun_chunks_total_count_data_dir + dir + ".xlsx"):
            files = load_files(dir)
            # 名詞句の出現回数をカウントするための辞書
            token_count = {}
            for file in files:
                noun_chunks_extract(dir, file, token_count)
            # ディレクトリ内の全作品の名詞句出現回数を合計したものを降順にソート
            sorted_token_count = sorted(token_count.items(), key=lambda x: x[1], reverse=True)
            # xlsx形式で保存
            save_xlsx_total(dir, sorted_token_count)
        else:
            pass


make_count_dir()
main_function()