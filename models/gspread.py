import re
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe
from settings.setting import ENGINE

COLUMNS = ['date', 'shop', 'product_name', 'price', 'shipping_cost',
           'quantity', 'asin', 'unit_price', 'rate', 'product_url']


class Gspread(object):
    def __init__(self):
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                       'https://spreadsheets.google.com/feeds',
                       'https://www.googleapis.com/auth/drive']
        self.json_key_file = 'pelagic-region-134805-4a53d343e113.json'
        self.sheet_id = '1a10YNZ3knukv-PwW6beSWTMCyWhVY7Nci6danY1H1xQ'

    def get_pharaoh(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.json_key_file, self.scopes)
        # gspread用に認証
        gc = gspread.authorize(credentials)
        # スプレッドシートのIDを指定してワークブックを選択
        workbook = gc.open_by_key(self.sheet_id)
        # 全てのworksheetを取得
        worksheet_list = workbook.worksheets()
        # "設定"等関連しないシートのみにする
        # products_worksheet_list = worksheet_list[2:len(worksheet_list) - 7]
        products_worksheet_list = worksheet_list[11:len(worksheet_list) - 20]
        print(products_worksheet_list)
        for sht in products_worksheet_list:
            print(sht.title)
            sheet = workbook.worksheet(sht.title)
            # 商品の列(関数の利用あり)のみ取り出し
            df_product = get_as_dataframe(sheet, evaluate_formulas=False, usecols=[4], header=13, encoding='utf-8')
            # URL列を追加してデフォルト値Noneを設定
            df_product['URL'] = None
            # 商品名の列が空の行を削除
            df_product.dropna(subset=['商品名'], inplace=True)
            # Productが=HYPERLINKで始まっていれば商品名とURLを個別に取り出す
            df_url_name = self.get_url_name(df_product)
            # 商品名以外の列(関数利用なし)を取り出し
            df_all = get_as_dataframe(sheet, evaluate_formulas=True,
                                      usecols=[2, 3, 4, 5, 6, 7, 12, 19, 20],
                                      header=13, ncoding='utf-8')
            # 日付の列が空の行を削除
            df_all.dropna(subset=['日付'], inplace=True)
            # 商品名、商品URLのテーブルとそれ以外のテーブルを結合
            df_merged = pd.concat([df_all, df_url_name], axis=1)
            df_merged.columns = COLUMNS
            df_merged.to_sql(
                name='pharaoh',
                con=ENGINE,
                if_exists='append',
                index=False,
                chunksize=1000,
                method='multi'
            )

    def get_url_name(self, data):
        for row in data.itertuples():
            product = row[1]
            if product[:1] == '=':
                product_url = re.search('"([^"]*)"[^"]*"([^"]*)"', row[1]).group(1)
                # print('Product URLは', product_url)
                product_name = re.search('"([^"]*)"[^"]*"([^"]*)"', row[1]).group(2)
                # print('Product URLは', product_name)
                # print('row[0]は', row[0])
                data.iloc[row[0], 0] = product_name
                data.iloc[row[0], 1] = product_url
            else:
                data.iloc[row[0], 0] = product
        # 元の列の"商品名"の列と重複するので削除
        data.drop(columns=['商品名'], inplace=True)
        return data




