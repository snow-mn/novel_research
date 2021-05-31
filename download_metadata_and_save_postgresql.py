# 『なろう小説API』を用いて、なろうの『全作品情報データを一括取得する』Pythonスクリプト
# 2020-04-26更新
import requests
import pandas as pd
import json
import time as tm
import datetime
import gzip
from tqdm import tqdm
import os
import psycopg2
from sqlalchemy import create_engine

tqdm.pandas()

# リクエストの秒数間隔(1以上を推奨)
interval = 1

### なろう小説API・なろう１８禁小説API を設定 ####
is_narou = True

now_day = datetime.datetime.now()
now_day = now_day.strftime("%Y_%m_%d")

# データベースの接続情報
connection_config = {
    "host": "localhost",
    "port": "5432",
    "dbname": "narou_data",
    "user": "postgres",
    "password": "password"
}

if is_narou:
    api_url = "https://api.syosetu.com/novelapi/api/"
else:
    api_url = "https://api.syosetu.com/novel18api/api/"


#####　以上設定、以下関数　##############


# 全作品情報の取得
def get_all_novel_info():
    df = pd.DataFrame()
    # print(df)

    # 出力パラメータの設定
    payload = {'out': 'json', 'gzip': 5, 'of': 'n', 'lim': 1}
    # レスポンスをcontent形式で取得
    res = requests.get(api_url, params=payload).content
    # 解凍して展開、デコード
    # 出力例：[{"allcount":9},{"ncode":"N0423GU"}]
    r = gzip.decompress(res).decode("utf-8")
    # json文字列を辞書に変換して、対象作品数の値を取得して代入
    allcount = json.loads(r)[0]["allcount"]

    print('対象作品数  ', allcount);

    # キュー数、500で切り捨て除算（小数部分を切り捨て）
    all_queue_cnt = (allcount // 500) + 10

    # 現在時刻を取得
    nowtime = datetime.datetime.now().timestamp()
    lastup = int(nowtime)

    # 進捗表示しながら繰り返し処理
    for i in tqdm(range(all_queue_cnt)):
        payload = {'out': 'json', 'gzip': 5, 'opt': 'weekly', 'lim': 500, 'lastup': "1073779200-" + str(lastup)}

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

        # データを展開しutf-8に変換
        r = gzip.decompress(res).decode("utf-8")

        # pandasのデータフレームに追加する処理
        df_temp = pd.read_json(r)
        # 行番号0を削除（最初に追加したallcountの値のみがある行）
        df_temp = df_temp.drop(0)

        # キーワード列の値を空白文字で分割、リスト化
        df_temp["keyword"] = df_temp["keyword"].str.split(" ")

        # dfとdf_tempの連結（dfは最初は空だが徐々に増えていく）
        df = pd.concat([df, df_temp])

        # 後ろから一番目の行かつ列名が"general_lastup"の値を取得
        last_general_lastup = df.iloc[-1]["general_lastup"]

        # 文字列から日付、時間（datetimeオブジェクト）への変換後、timestamp()でUNIX時間に変換
        lastup = datetime.datetime.strptime(last_general_lastup, "%Y-%m-%d %H:%M:%S").timestamp()
        # intに型変換
        lastup = int(lastup)

        # 取得間隔を空ける
        tm.sleep(interval)

    # PostgreSQLに書き込む
    dump_to_postgresql(df)


# PostgreSQLに書き込む処理
def dump_to_postgresql(df):
    # allcount列を削除
    df = df.drop("allcount", axis=1)
    # gensaku列を削除
    df = df.drop("gensaku", axis=1)
    # 重複行を削除する
    df.drop_duplicates(subset='ncode', inplace=True)
    # インデックスの振り直し
    df = df.reset_index(drop=True)
    # 数値をint型に変換
    df[["userid", "biggenre", "genre", "novel_type", "end", "general_all_no", "length", "time", "isstop", "isr15", "isbl", "isgl", "iszankoku", "istensei", "istenni", "pc_or_k", "global_point", "daily_point", "weekly_point", "monthly_point", "quarter_point", "yearly_point", "fav_novel_cnt", "impression_cnt", "review_cnt", "all_point", "all_hyoka_cnt", "sasie_cnt", "kaiwaritu", "weekly_unique"]] = df[["userid", "biggenre", "genre", "novel_type", "end", "general_all_no", "length", "time", "isstop", "isr15", "isbl", "isgl", "iszankoku", "istensei", "istenni", "pc_or_k", "global_point", "daily_point", "weekly_point", "monthly_point", "quarter_point", "yearly_point", "fav_novel_cnt", "impression_cnt", "review_cnt", "all_point", "all_hyoka_cnt", "sasie_cnt", "kaiwaritu", "weekly_unique"]].astype("int")
    # エクスポート開始の合図
    print("export_start", datetime.datetime.now())
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))

    # PostgreSQLのテーブルにDataFrameを追加する
    df.to_sql("metadata", con=engine, if_exists='append', index=False)
    print('取得成功数  ', len(df));
    print("データベースにデータを保存しました")


#######　関数の実行を指定　##########
print("start", datetime.datetime.now())

get_all_novel_info()

print("end", datetime.datetime.now())
