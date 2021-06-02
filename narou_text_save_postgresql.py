# 本文テキストをpostgresqlに格納するプログラム

import time
import re
from urllib import request
from urllib import error
from bs4 import BeautifulSoup
import pandas as pd
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


# postgreSQLからデータを取得
def get_postgresql_data(connection):
    # DataFrameでロード（総合評価ポイント降順にソート）
    df = pd.read_sql(sql="SELECT ncode, novel_type FROM metadata ORDER BY global_point DESC;", con=connection)
    return df


# 既に本文データがPostgreSQLに格納されているかの判定
def check_existed(connection, ncode):
    cur = connection.cursor()
    # 既に格納されていればTrueを返すSQL文
    cur.execute("SELECT EXISTS (SELECT * FROM text_data WHERE ncode = '%s');" % ncode)
    # 結果の取得
    result = cur.fetchone()
    # curを閉じる
    cur.close()
    return result


# 小説本文の取得
def fetch_novel(ncode):
    # 例外処理
    try:
        # 全部分数を取得
        info_url = "https://ncode.syosetu.com/novelview/infotop/ncode/{}/".format(ncode)
        info_res = request.urlopen(info_url)
        soup = BeautifulSoup(info_res, "html.parser")
        pre_info = soup.select_one("#pre_info").text
        # 3桁区切りも考慮して全部分数の取得
        num_parts = re.search(r"全([0-9,]+)部分", pre_info).group(1)
        # カンマの削除
        num_parts = int(re.sub(r",", "", num_parts))
        # 本文を入れる変数
        zenbun = ""
        # 本文取得
        for part in tqdm(range(1, num_parts + 1)):
            # 作品本文ページのURL
            url = "https://ncode.syosetu.com/{}/{:d}/".format(ncode, part)
            res = request.urlopen(url)
            soup = BeautifulSoup(res, "html.parser")
            # CSSセレクタで本文を指定
            honbun = soup.select_one("#novel_honbun").text
            # 次の部分との間は念のため改行しておく
            honbun += "\n"
            # 本文に追加
            zenbun = zenbun + honbun
            # 進捗を表示
            # print("part {:d} downloaded (total: {:d} parts)".format(part, num_parts))
            # 次の部分取得までは1秒間の時間を空ける
            time.sleep(0.01)
    except error.HTTPError as e:
        print("エラー : ", e)

    # 空白文字を取り除く（全角スペース、半角スペース①,②、タブ文字）
    zenbun = re.sub(r"[\u3000\u0020\u00A0\t]", "", zenbun)
    # 改行文字で分割してリスト化
    lines = zenbun.split("\n")
    # 空白行を削除
    result = [line for line in lines if line != ""]
    # print(result)
    print("%sの長さは%s行でした" % (ncode, len(result)))
    return result


# 小説本文の取得（短編用）
def fetch_novel2(ncode):
    # 例外処理
    try:
        # 本文取得
        # 作品本文ページのURL
        url = "https://ncode.syosetu.com/{}/".format(ncode)
        res = request.urlopen(url)
        soup = BeautifulSoup(res, "html.parser")
        # CSSセレクタで本文を指定
        zenbun = soup.select_one("#novel_honbun").text
    except error.HTTPError as e:
        print("エラー : ", e)

    # 空白文字を取り除く（全角スペース、半角スペース①,②、タブ文字）
    zenbun = re.sub(r"[\u3000\u0020\u00A0\t]", "", zenbun)
    # 改行文字で分割してリスト化
    lines = zenbun.split("\n")
    # 空白行を削除
    result = [line for line in lines if line != ""]
    print(result)
    print("%sの長さは%s行でした" % (ncode, len(result)))
    return result


# 本文データをPostgreSQLに格納する関数
def dump_to_postgresql(ncode, lines):
    # DataFrameの作成
    df = pd.DataFrame(columns=["ncode", "honbun"])
    # DataFrameに追加
    df.loc[0] = [ncode, lines]
    # データベース接続の準備
    engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{dbname}".format(**connection_config))

    # PostgreSQLのテーブルにDataFrameを追加する
    df.to_sql("text_data", con=engine, if_exists='append', index=False)
    print("データベースにデータを保存しました")


# メイン関数
def main():
    print("PostgreSQLに接続しています")
    # PostgreSQLに接続
    connection = psycopg2.connect(**connection_config)
    # PostgreSQLからメタデータの取得
    df = get_postgresql_data(connection)
    # ncodeのリスト
    ncode_list = df["ncode"].to_list()
    # novel_typeのリスト
    novel_type_list = df["novel_type"].to_list()
    # (ncode, novel_type)とするためのリスト
    data_list = []
    # データ数だけ繰り返す
    for i in range(len(ncode_list)):
        # (ncode, novel_type)を追加していく
        data_list.append([ncode_list[i], novel_type_list[i]])
    # データリストからncode,novel_typeを順々に読み込んでいく
    for ncode, novel_type in data_list:
        # 該当作品の本文データが既にデータベースに存在していなければ
        if not check_existed(connection, ncode)[0]:
            print("%sの小説本文のダウンロードを開始します" % ncode)
            if novel_type == 1:
                lines = fetch_novel(ncode)
            elif novel_type == 2:
                lines = fetch_novel2(ncode)
            else:
                pass
            # PostgreSQLに本文データを格納
            dump_to_postgresql(ncode, lines)
        else:
            print("%sの本文データは既にデータベースに存在しています" % ncode)


# 実行
main()