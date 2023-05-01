import re
import time

from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe
from settings.setting import ENGINE
from sqlalchemy.types import *

COLUMNS = ['date', 'shop', 'product_name', 'price', 'shipping_cost',
           'quantity', 'asin', 'unit_price', 'rate', 'product_url',
           'amazon_link', 'keepa_link', 'pricestar_url', 'bought_by']
# COLUMNS = ['id', 'date', 'shop', 'product_name', 'price', 'shipping_cost',
#            'quantity', 'asin', 'unit_price', 'rate', 'product_url',
#            'amazon_link', 'keepa_link', 'pricestar_url', 'bought_by']


class Gspread(object):
    def __init__(self):
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
                       'https://spreadsheets.google.com/feeds',
                       'https://www.googleapis.com/auth/drive']
        self.json_key_file = 'pelagic-region-134805-4a53d343e113.json'
        self.sheet_id = [
            # ('1a10YNZ3knukv-PwW6beSWTMCyWhVY7Nci6danY1H1xQ', 'rakuten'),
            # ('1tmAY-eYUg9ew6EjoYJXupfLNaVvUKOEfddxj8ryPz7A', 'rakuten'),
            # ('1ME9wBDUzar_Ccix4FzXdv8I0QmmwwhNN2oHz0GdHupY', 'yahoo'),
            # ('1kCZac_0EWw2-HxqpzuxE0DRb6hS6tJsSDRoqvJS2DBU', 'yahoo')
            ('11_X8pwIxG2ZOFDmb4q9DfX644tvhD59yGIzfbh8RPFc', 'rakuten')
        ]

    def get_pharaoh(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.json_key_file, self.scopes)
        # gspread用に認証
        gc = gspread.authorize(credentials)

        # 楽天、ヤフショのシートIDに対し読み込み
        for sheet_info in self.sheet_id:
            # スプレッドシートのIDを指定してワークブックを選択
            workbook = gc.open_by_key(sheet_info[0])
            # 全てのworksheetを取得
            worksheet_list = workbook.worksheets()
            print('シートの数は', len(worksheet_list))
            # "設定"等関連しないシートのみにする
            products_worksheet_list = worksheet_list[2:len(worksheet_list) - 7]
            # products_worksheet_list = worksheet_list[11:len(worksheet_list) - 20]
            print(products_worksheet_list)
            for sht in products_worksheet_list:
                print(sht.title)
                sheet = workbook.worksheet(sht.title)
                # 商品の列(関数の利用あり)のみ取り出し
                df_product = get_as_dataframe(sheet, evaluate_formulas=False, usecols=[4], header=13, encoding='utf-8')
                # 商品名の列が空の行を削除
                df_product.dropna(subset=['商品名'], inplace=True)
                # Productが=HYPERLINKで始まっていれば商品名とURLを個別に取り出す
                df_url_name = self.get_url_name(df_product)
                # 商品名以外の列(関数利用なし)を取り出し
                df_all = get_as_dataframe(sheet, evaluate_formulas=True,
                                          usecols=[2, 3, 4, 5, 6, 7, 12, 19, 20],
                                          header=13, encoding='utf-8')
                # @購入単価の列が空の行を削除
                df_all.dropna(subset=['@購入単価'], inplace=True)
                # 商品名、商品URLのテーブルとそれ以外のテーブルを結合
                df_merged = pd.concat([df_all, df_url_name], axis=1)
                # 足りない情報を追加(Amazon URL, Keepa URL, Pricestar URL, Bought by)
                # 購入元(楽天orヤフショ)を渡す sheet_info[1]に'rakuten'か'yahoo'が入ってる
                self.make_url(df_merged, sheet_info[1])

                # # # プライマリーキーとなるidの列を追加
                # df_merged.insert(0, 'id', 0)

                # DBに書き込む時の型の指定
                df_schema = {
                    "id": Integer,
                    "date": DateTime,
                    "shop": String(100),
                    "product_name": String(255),
                    "price": Integer,
                    "shipping_cost": Integer,
                    "quantity": Integer,
                    "asin": String(20),
                    "unit_price": Integer,
                    "rate": Float(1),
                    "product_url": String(255),
                    "amazon_link": String(255),
                    "keepa_link": String(255),
                    "pricestar_url": String(255),
                    "bought_by": String(20)
                }

                df_merged.columns = COLUMNS
                df_merged.to_sql(
                    name='pharaoh',
                    con=ENGINE,
                    if_exists='append',
                    index=False,
                    # chunksize=1000,
                    method='multi',
                    dtype=df_schema
                )
                with ENGINE.connect() as conn:
                    conn.execute("""
                    ALTER TABLE pharaoh ADD PRIMARY KEY(id);
                    """)
                time.sleep(5)

    def get_url_name(self, data):
        # URL列を追加してデフォルト値Noneを設定
        data['URL'] = None
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

    def make_url(self, df, by):
        # amazon_link、keepa_link、pristar_url、bought_by列を追加してデフォルト値Noneを設定
        df['amazon_link'] = None
        df['keepa_link'] = None
        df['pricestar_url'] = None
        df['bought_by'] = None
        amazon_ep = 'https://www.amazon.co.jp/dp/'
        keepa_ep = 'https://keepa.com/#!search/5-'
        pricestar_ep = 'https://jp2.pricetar.com/seller/orders/orderlist?searchMethod=keyword&inventoryStock%5Bm%5D=&keyword='
        for row in df.itertuples():
            if isinstance(row[7], float):
                df.iloc[row[0], 10] = None
                df.iloc[row[0], 11] = None
            # elif isinstance(row[7], str) and re.search('\(|（', row[7]):
            else:
                asin = row[7][0:10]
                amazon_url = amazon_ep + asin
                keepa_url = keepa_ep + asin
                pricestar_url = pricestar_ep + asin
                df.iloc[row[0], 10] = amazon_url
                df.iloc[row[0], 11] = keepa_url
                df.iloc[row[0], 12] = pricestar_url
                df.iloc[row[0], 13] = by
                df.iloc[row[0], 6] = asin
        return df






