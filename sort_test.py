import win32com.client

file_path = r"C:\Users\user\Desktop\syukujitsu.xlsx"

# Excelファイル操作のための準備
excel = win32com.client.Dispatch("Excel.Application")
wb = excel.Workbooks.Open(Filename=file_path)

# Sort 数値の割り振り
# http://www.eurus.dti.ne.jp/~yoneyama/Excel/vba/vba_sort.html
xlAscending = 1
xlDescendig = 2
xlYes = 1

# B列で昇順
wb.Sheets(1).Columns("A:B").Sort(Key1=wb.Sheets(1).Range('B2'), Order1=xlAscending, Header=xlYes)
# A列で降順
wb.Sheets(1).Columns("A:B").Sort(Key1=wb.Sheets(1).Range('A2'), Order1=xlDescendig, Header=xlYes)

# Excelファイルを保存
wb.Save()

# Excelを閉じる
excel.Quit()