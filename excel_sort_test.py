import pandas as pd

data_dir = "Data/"
file_name = "Narou_All_OUTPUT_1000pt_min100000length_kai.xlsx"


# DataFrameにExcelの読み込み
df = pd.read_excel(data_dir + file_name, sheet_name='Sheet1', engine='openpyxl')

# Z列のglobal_pointを基準にして降順(False)・昇順（True）ができる
df_a = df.sort_values('global_point', ascending=False)

# ExcelWriterを追加することで、SaveとCloseの必要がなくなる。またengineをopenpyxlに指定ができる
with pd.ExcelWriter(data_dir + file_name, engine='openpyxl', mode='a') as writer:
    # A列の国民の祝日・休日月日を基準にして降順(False)にしたデータをresultシートに追加
    df_a.to_excel(writer, sheet_name='result', index=False)