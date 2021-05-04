# 『なろう小説API』を用いて、なろうの『全作品情報データを一括取得する』Pythonスクリプト
# 取得したメタデータを総合評価順にソート、またキーワードの出現回数もカウントする

import requests
import pandas as pd
import json
import time as tm
import datetime
import gzip
from tqdm import tqdm
import numpy as np
import csv
import openpyxl

tqdm.pandas()
import xlsxwriter
import sqlite3

# ディレクトリ
data_dir = "../novel_Data/"
text_dir = "../novel_Text/"

# リクエストの秒数間隔(1以上を推奨)
interval = 2

### なろう小説API・なろう１８禁小説API を設定 ####
is_narou = True

now_day = datetime.datetime.now()
now_day = now_day.strftime("%Y_%m_%d")

if is_narou:
    filename = 'Narou_All_OUTPUT_%s.xlsx' % now_day
    sql_filename = 'Narou_All_OUTPUT_%s.sqlite3' % now_day
    api_url = "https://api.syosetu.com/novelapi/api/"
else:
    filename = 'Narou_18_ALL_OUTPUT_%s.xlsx' % now_day
    sql_filename = 'Narou_18_ALL_OUTPUT_%s.sqlite3' % now_day
    api_url = "https://api.syosetu.com/novel18api/api/"

# データをSqlite3形式でも保存する
is_save_sqlite = False


#####　以上設定、以下関数　##############

# 全作品情報の取得
def get_all_novel_info():
    df = pd.DataFrame()

    # 出力パラメータの設定
    # 総合ポイントの下限と文字数の上限を設定
    payload = {'out': 'json', 'gzip': 5, 'of': 'n', 'lim': 1, "min_globalpoint": 1000, "length": -100000}
    # レスポンスをcontent形式で取得
    res = requests.get(api_url, params=payload).content
    # 解凍して展開、デコード
    r = gzip.decompress(res).decode("utf-8")
    # json文字列を辞書に変換して、対象作品数の値を取得して代入
    allcount = json.loads(r)[0]["allcount"]

    print('対象作品数  ', allcount);

    # キュー数、500で切り捨て除算
    all_queue_cnt = (allcount // 500) + 10

    # 現在時刻を取得
    nowtime = datetime.datetime.now().timestamp()
    lastup = int(nowtime)

    # 進捗表示しながら繰り返し処理
    for i in tqdm(range(all_queue_cnt)):
        payload = {'out': 'json', 'gzip': 5, 'opt': 'weekly', 'lim': 500, 'lastup': "1073779200-" + str(lastup), "min_globalpoint": 1000, "length": -100000}

        # なろうAPIにリクエスト
        cnt = 0
        while cnt < 5:
            try:
                res = requests.get(api_url, params=payload, timeout=30).content
                break
            except:
                print("Connection Error")
                cnt = cnt + 1
                tm.sleep(120)  # 接続エラーの場合、120秒後に再リクエストする

        r = gzip.decompress(res).decode("utf-8")

        # pandasのデータフレームに追加する処理
        df_temp = pd.read_json(r)
        df_temp = df_temp.drop(0)

        df = pd.concat([df, df_temp])

        last_general_lastup = df.iloc[-1]["general_lastup"]

        lastup = datetime.datetime.strptime(last_general_lastup, "%Y-%m-%d %H:%M:%S").timestamp()
        lastup = int(lastup)

        # 取得間隔を空ける
        tm.sleep(interval)

    dump_to_excel(df)


# エクセルファイルに書き込む処理
def dump_to_excel(df):
    # allcount列を削除
    df = df.drop("allcount", axis=1)

    # 重複行を削除する
    df.drop_duplicates(subset='ncode', inplace=True)
    df = df.reset_index(drop=True)

    print("export_start", datetime.datetime.now())

    try:
        # .xlsx ファイル出力
        writer = pd.ExcelWriter(data_dir + filename, options={'strings_to_urls': False}, engine='xlsxwriter')
        df.to_excel(writer, sheet_name="Sheet1")  # Writerを通して書き込み
        writer.close()

        print('取得成功数  ', len(df));

    except:
        pass

    ### SQLite3に書き込む処理 (将来エクセルの上限行数を超えたときのため) ###
    if is_save_sqlite == True or len(df) >= 1048576:
        # 接続DBファイルの指定
        conn = sqlite3.connect(sql_filename)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        df.to_sql('novel_data', conn, if_exists='replace')

        c.close()
        conn.close()

        print("Sqlite3形式でデータを保存しました")

    sort_excel(data_dir + filename)


# メタデータファイルを総合評価ポイントの降順にソート
def sort_excel(file_path):
    # DataFrameにExcelの読み込み
    df = pd.read_excel(file_path, sheet_name='Sheet1', engine='openpyxl')

    # Z列のglobal_pointを基準にして降順(False)・昇順（True）ができる
    df_a = df.sort_values('global_point', ascending=False)

    # ExcelWriterを追加することで、SaveとCloseの必要がなくなる。またengineをopenpyxlに指定ができる
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
        # A列の国民の祝日・休日月日を基準にして降順(False)にしたデータをresultシートに追加
        df_a.to_excel(writer, sheet_name='sorted', index=False)

    count_word_ranking(file_path)


# キーワードの出現回数のランキングを調べる
def count_word_ranking(file_path):
    # エクセルデータの取得
    wb = openpyxl.load_workbook(file_path)
    ws = wb["Sheet1"]

    # キーワードと出現回数を入れる辞書の作成
    keywords_dict = {}

    # 2行目から1行ごとに読み込む
    for row in ws.iter_rows(min_row=2):
        # キーワードの取得
        keywords = row[9].value
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
    sorted_dict = sorted(keywords_dict.items(), key=lambda x: x[1], reverse=True)
    # print(sorted_dict)

    # ファイル生成
    keywordcount_csv_path = data_dir + "keyword_count.csv" % now_day
    with open(keywordcount_csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "count"])
    # 200位までを書き込む
    for key, count in sorted_dict[0:200]:
        with open(data_dir + filename, "a", newline="", errors="ignore") as f:
            writer = csv.writer(f)
            writer.writerow([key, count])



#######　関数の実行を指定　##########
print("start", datetime.datetime.now())

get_all_novel_info()

print("end", datetime.datetime.now())
