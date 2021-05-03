import openpyxl

text_dir = "Text/"
data_dir = "Data/"
narou_data_path = data_dir + "Narou_All_OUTPUT_1000pt_kai.xlsx"
csv_path = data_dir + "keyword_count_1000pt_200.csv"

# エクセルデータファイルの取得
wb = openpyxl.load_workbook(narou_data_path)
ws = wb["Sheet1"]

for row in ws.iter_rows(min_row=2):
    # 各種メタデータの値を取得
    title = row[1].value
    ncode = row[2].value
    story = row[5].value
    biggenre = row[6].value
    genre = row[7].value
    keyword = row[9].value
    general_all_no = row[14].value
    # 代入
    metadata = [title, ncode, story, biggenre, genre, keyword, general_all_no]
    print(metadata)
