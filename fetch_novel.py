import time
import re
from urllib import request
from bs4 import BeautifulSoup

ncode = "n2443gu"  # 取得したい小説のNコードを指定

# 全部分数を取得
info_url = "https://ncode.syosetu.com/novelview/infotop/ncode/{}/".format(ncode)
info_res = request.urlopen(info_url)
soup = BeautifulSoup(info_res, "html.parser")
pre_info = soup.select_one("#pre_info").text
num_parts = int(re.search(r"全([0-9]+)部分", pre_info).group(1))

with open(ncode + ".txt", "w", encoding="utf-8") as f:
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