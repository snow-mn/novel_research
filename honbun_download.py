import os
import csv
import requests
import time
import re
from urllib import request
from bs4 import BeautifulSoup

text_dir = "Text/"
data_dir = "Data/"
csv_path = "keyword_count2.csv"

# csvの読み込み
def read_csv(csv_path):
    csv_file = open(data_dir + csv_path, "r", errors="", newline="")
    f = csv.reader(csv_file, delimiter=",", doublequote=True, lineterminator="\r\n", quotechar='"',
                   skipinitialspace=True)
    header = next(f)
    # print(header)
    count = 0
    for row in f:
        count += 1
        if count > 100:
            exit()
        else:
            # rowはList
            # row[0]で必要な項目を取得することができる
            get_metadata(row[0])

# ディレクトリ生成
def make_dir(keyword):
    path = text_dir + keyword
    os.mkdir(path)

# メタデータの取得
def get_metadata(keyword):
    # なろうAPI
    payload = {"out": "json", "lim": 500, "minlen": 10000, "maxlen": 100000, "order": "quarterpoint"}
    r = requests.get("https://api.syosetu.com/novelapi/api/", params=payload)
    x = r.json()

    # キーワードのディレクトリが存在していないなら作成
    if os.path.exists(text_dir + keyword):
        pass
    else:
        make_dir(keyword)

    # メタデータの取得
    for n in x:
        if len(n) > 1:
            metadata = [n["ncode"], n["title"], n["genre"], n["keyword"], n["story"], n["general_all_no"]]
            keywords = metadata[3].split(" ")
            # 該当キーワードが付与されている小説かつ掲載部分数が1より大きい（短編ではない）
            if keyword in keywords and metadata[5] > 1:
                # ディレクトリ内のファイル数が10を超えないように
                if sum(os.path.isfile(os.path.join(text_dir + keyword, name)) for name in os.listdir(text_dir + keyword)) < 10:
                    if os.path.exists(text_dir + keyword + "/" + metadata[0] + ".txt"):
                        pass
                    else:
                        print(metadata[1] + "のダウンロードを開始します")
                        fetch_novel(keyword, metadata[0])
                        print(metadata[1] + "のダウンロードが終了しました")
                else:
                    pass
            else:
                pass

# 小説本文の取得
def fetch_novel(keyword, ncode):
    # 全部分数を取得
    info_url = "https://ncode.syosetu.com/novelview/infotop/ncode/{}/".format(ncode)
    info_res = request.urlopen(info_url)
    soup = BeautifulSoup(info_res, "html.parser")
    pre_info = soup.select_one("#pre_info").text
    num_parts = int(re.search(r"全([0-9]+)部分", pre_info).group(1))

    with open(text_dir + keyword + "/" + ncode + ".txt", "w", encoding="utf-8") as f:
        for part in range(1, num_parts + 1):
            # 作品本文ページのURL
            url = "https://ncode.syosetu.com/{}/{:d}/".format(ncode, part)

            res = request.urlopen(url)
            soup = BeautifulSoup(res, "html.parser")

            # CSSセレクタで本文を指定
            honbun = soup.select_one("#novel_honbun").text
            honbun += "\n"  # 次の部分との間は念のため改行しておく

            # 保存
            f.write(honbun)

            print("part {:d} downloaded (total: {:d} parts)".format(part, num_parts))  # 進捗を表示

            time.sleep(1)  # 次の部分取得までは1秒間の時間を空ける

read_csv(csv_path)