# トークンを抽出して出現回数をカウントし、エクセルファイルで出力するプログラム
# 同一キーワードの作品全てについてトークンの出現回数を合計する

import spacy
import os
import pandas as pd
import numpy as np
import tqdm

# 抽出するトークンを設定
extract_target = "NOUN"

# パス
data_dir = "../novel_Data/"
text_dir = "../novel_Text/"
token_count_data_dir = data_dir + extract_target + "_count_data/"
token_total_count_data_dir = data_dir + extract_target + "_total_count_data/"


# 抽出する品詞のデータディレクトリがない場合は作成する関数
def make_count_dir(extraxt_target):
    if not os.path.exists(data_dir + extract_target + "_count_data"):
        os.mkdir(data_dir + extract_target + "_count_data")
    else:
        pass
    if not os.path.exists(data_dir + extract_target + "_total_count_data"):
        os.mkdir(data_dir + extract_target + "_total_count_data")
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
    if os.path.exists(token_count_data_dir + keyword):
        pass
    else:
        os.mkdir(token_count_data_dir + keyword)
    # ファイルパス
    xlsx_path = token_count_data_dir + keyword + "/" + file_name.replace(".txt", "") + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_dict, columns=[extract_target, "count"])
    print(df)
    # エクセルファイルを作成・保存
    df.to_excel(xlsx_path, sheet_name="token_count")


# トークン出現回数をexcelファイルに保存（全作品合計）
def save_xlsx_total(keyword, sorted_token_count):
    # ファイルパス
    xlsx_path = token_total_count_data_dir + keyword + ".xlsx"
    # データフレーム化
    df = pd.DataFrame(sorted_token_count, columns=[extract_target, "count"])
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
    for line in tqdm.tqdm(read_data):
        doc = nlp(line)
        for tok in doc:
            # print(tok.text)
            if tok.pos_ == extract_target:
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
        if not os.path.exists(token_total_count_data_dir + dir + ".xlsx"):
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