import os
import csv

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
            print(row)

read_csv(csv_path)