import os
import csv
import time
import re
from urllib import request
from urllib import error
from bs4 import BeautifulSoup
import openpyxl
from tqdm import tqdm

data_dir = "../novel_Data/"
text_dir = "../novel_Text/"
narou_data_path = data_dir + "Narou_All_OUTPUT_2021_05_04.xlsx"
csv_path = data_dir + "keyword_count_2021_05_04.csv"


# これを最初に呼び出す
def first_function(csv_path):
    csv_file = open(csv_path, "r", errors="", newline="")
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
            # キーワードのディレクトリが存在していないなら作成
            if os.path.exists(text_dir + row[0]):
                pass
            else:
                make_dir(row[0])
            # キーワードディレクトリ内に既に100個のファイルがあるならパス
            if sum(os.path.isfile(os.path.join(text_dir + row[0], name)) for name in os.listdir(text_dir + row[0])) < 100:
                # rowはList
                # row[0]で必要な項目を取得することができる
                second_function(row[0])
            else:
                pass


# ディレクトリ生成
def make_dir(keyword):
    path = text_dir + keyword
    os.mkdir(path)


# 2段階目の関数
# なろうメタデータファイルからメタデータを取得して小説本文のダウンロード
def second_function(keyword):
    # エクセルデータファイルの取得
    wb = openpyxl.load_workbook(narou_data_path)
    ws = wb["sorted"]
    # 行ごとに取得
    for row in tqdm(ws.iter_rows(min_row=2)):
        # 各種メタデータの値の取得
        title = row[1].value
        ncode = row[2].value
        keyword_text = row[9].value
        general_all_no = row[14].value
        length = row[15].value

        # キーワードが存在するなら分割
        if keyword_text is not None:
            keywords = keyword_text.split(" ")

            # 該当キーワードが付与されている小説かつ掲載部分数が1より大きい（短編ではない）
            # 文字数に条件を設けるか
            if keyword in keywords and general_all_no > 1 and length < 100000:
                # ディレクトリ内のファイル数が100を超えないように
                if sum(os.path.isfile(os.path.join(text_dir + keyword, name)) for name in os.listdir(text_dir + keyword)) < 100:
                    if os.path.exists(text_dir + keyword + "/" + ncode + ".txt"):
                        pass
                    else:
                        print(ncode + ":" + title + "のダウンロードを開始します")
                        fetch_novel(keyword, ncode)
                        print(ncode + ":" + title + "のダウンロードが終了しました")
                else:
                    pass
            else:
                pass
        else:
            pass



# 小説本文の取得
def fetch_novel(keyword, ncode):
    # 例外処理
    try:
        # 全部分数を取得
        info_url = "https://ncode.syosetu.com/novelview/infotop/ncode/{}/".format(ncode)
        info_res = request.urlopen(info_url)
        soup = BeautifulSoup(info_res, "html.parser")
        pre_info = soup.select_one("#pre_info").text
        num_parts = int(re.search(r"全([0-9]+)部分", pre_info).group(1))
        # 本文取得
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

                time.sleep(0.1)  # 次の部分取得までは1秒間の時間を空ける
    except error.HTTPError as e:
        print("エラー : ", e)

first_function(csv_path)