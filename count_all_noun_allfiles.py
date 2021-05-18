# 全ての名詞（NOUN、を抽出して出現回数をカウントし、エクセルファイルで出力するプログラム
# 同一キーワードの作品全てについてトークンの出現回数を合計する

import spacy
import os
import pandas as pd
import numpy as np
import tqdm
import time

# パス
data_dir = "Data/"
text_dir = "Text/"
all_noun_count_data_dir = data_dir + "all_noun_count_data/"
all_noun_total_count_data_dir = data_dir + "all_noun_total_count_data/"

# 抽出対象
# NOUN：名詞, PROPN：固有名詞, PRON：代名詞
extract_target = ["NOUN", "PROPN"]


# 抽出する品詞のデータディレクトリがない場合は作成する関数
def make_count_dir(extraxt_target):
    if not os.path.exists(all_noun_count_data_dir):
        os.mkdir(all_noun_count_data_dir)
    else:
        pass
    if not os.path.exists(all_noun_total_count_data_dir):
        os.mkdir(all_noun_total_count_data_dir)
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


# トークン出現回数をexcelファイルに保存（作品個別用）
def save_xlsx(keyword, file_name, sorted_dict):
    # キーワードのディレクトリが存在していないなら作成
    if os.path.exists(all_noun_count_data_dir + keyword):
        pass
    else:
        os.mkdir(all_noun_count_data_dir + keyword)
    # ファイルパス
    xlsx_path = all_noun_count_data_dir + keyword + "/" + file_name.replace(".txt", "") + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_dict, columns=["token", "count"])
    print(df)
    # エクセルファイルを作成・保存
    df.to_excel(xlsx_path, sheet_name="token_count")


# トークン出現回数をexcelファイルに保存（全作品合計）
def save_xlsx_total(keyword, sorted_token_count):
    # ファイルパス
    xlsx_path = all_noun_total_count_data_dir + keyword + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_token_count, columns=["token", "count"])
    print(df)
    # エクセルファイルを作成・保存
    df.to_excel(xlsx_path, sheet_name="token_count")
    print(xlsx_path + "の保存が完了しました")


# トークン抽出
def token_extract(dir_name, file_name, token_count):
    # ファイルパス
    file_path = text_dir + dir_name + "/" + file_name
    # モデルのロード
    nlp = spacy.load("ja_ginza")
    # テキストファイルの読み込み
    with open(file_path, mode="rt", encoding="utf-8") as f:
        read_data = f.readlines()
        # print(read_data)
    # 1行毎に形態素解析
    print(file_path + "の形態素解析を開始します")
    time.sleep(0.1)
    for line in tqdm.tqdm(read_data):
        doc = nlp(line)
        for tok in doc:
            # print(tok.text)
            if tok.pos_ in extract_target:
                if tok.text in token_count:
                    token_count[tok.text] = token_count[tok.text] + 1
                else:
                    token_count[tok.text] = 1
            else:
                pass


# メイン関数
def main_function():
    dirs = load_dirs()
    for dir in dirs:
        # 既にエクセルデータが存在するか
        if not os.path.exists(all_noun_total_count_data_dir + dir + ".xlsx"):
            files = load_files(dir)
            # トークンの出現回数をカウントするための辞書
            token_count = {}
            for file in files:
                token_extract(dir, file, token_count)
            # ディレクトリ内の全作品のトークン出現回数を合計したものを降順にソート
            sorted_token_count = sorted(token_count.items(), key=lambda x: x[1], reverse=True)
            # xlsx形式で保存
            save_xlsx_total(dir, sorted_token_count)
        else:
            pass

make_count_dir(extract_target)
main_function()