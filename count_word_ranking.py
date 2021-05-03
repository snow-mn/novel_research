import pandas as pd
import numpy as np
import csv

# ディレクトリ
data_dir = "Data/"

# データファイル
# file = pd.ExcelFile(data_dir + "Narou_All_OUTPUT_1000pt.xlsx")
file = pd.ExcelFile(data_dir + "Narou_All_OUTPUT_1000pt_min100000length.xlsx")

# シート取得
sheet_def = file.parse("Sheet1", header=None)

# n行目の9列目（キーワード）を取得、返り値はseries

keywords_dict = {}

for n, a in sheet_def.iterrows():
    # print(n + "行目")
    # キーワード取得
    keywords = sheet_def.iloc[n][9]
    # print(keywords)
    # キーワードが存在しないならパス
    if keywords is np.nan:
        # print("キーワードがありませんでした")
        pass
    # キーワドが存在するなら
    else:
        # " "を区切り文字として分割してリスト化
        keywords_list = keywords.split(" ")
        # print(keywords_list)
        # 辞書内の各キーワードについて
        for key in keywords_list:
            # print(key)
            # 辞書内にキーワードが含まれていなければ追加、含まれていれば回数+1
            if key in keywords_dict:
                keywords_dict[key] = keywords_dict[key] + 1
            else:
                keywords_dict[key] = 1
# print(keywords_dict)
# 出現回数降順にソート
# keywords_dict = {key, count for key, count in keywords_dict.items() if count >= 10000}
sorted_dict = sorted(keywords_dict.items(), key=lambda x:x[1], reverse=True)
print(sorted_dict)

# ファイル生成
filename = "keyword_count_1000pt_min100000length_200.csv"
with open(data_dir + filename, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["keyword", "count"])

for key, count in sorted_dict[0:200]:
    with open(data_dir + filename, "a", newline="", errors="ignore") as f:
        writer = csv.writer(f)
        writer.writerow([key, count])