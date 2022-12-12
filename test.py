import re
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# product = '=HYPERLINK("https://item.rakuten.co.jp/hatasan01/tbp4903sp/?s-id=ph_pc_itemname","KTC（ケーティーシー）　1/2 インパクトレンチ用ホイールナットソケットセット3ピース（17,19,21mm）　★TBP4903")'
# pattern = '[\w/:%#$&\?\(\)~\.=\+\-]+'

# url = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', product)
# print(url)
#
# url = re.search('"([^"]*)"[^"]*"([^"]*)"', product)
# print(url.group(0))
# print(url.group(1))
# print(url.group(2))

scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
          'https://spreadsheets.google.com/feeds',
          'https://www.googleapis.com/auth/drive']
json_key_file = 'pelagic-region-134805-4a53d343e113.json'
sheet_id = '1a10YNZ3knukv-PwW6beSWTMCyWhVY7Nci6danY1H1xQ'

credentials = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scopes)
# gspread用に認証
gc = gspread.authorize(credentials)
# スプレッドシートのIDを指定してワークブックを選択
workbook = gc.open_by_key(sheet_id)

worksheet_list = workbook.worksheets()
last_sheet = len(worksheet_list)-7
products_worksheet_list = worksheet_list[2:len(worksheet_list)-7]
print(products_worksheet_list)

for sht in products_worksheet_list:
    print(sht.title)


