import pandas as pd

file = pd.ExcelFile("Narou_All_OUTPUT.xlsx")

# シート取得
sheet_def = file.parse("Sheet1", header=None)

# n行目を取得、返り値はseries
print(sheet_def.iloc[1])
# n行目の1列目
print(sheet_def.iloc[1][1])