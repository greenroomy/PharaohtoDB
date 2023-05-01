from sqlalchemy import select, func
from settings.setting import session
from models.history import History

ASIN = 'B07KCG1WMJ'

all_data = session.query(
    func.group_concat(History.product_name.distinct()).label('product_name'),
    func.group_concat(History.product_url.distinct()).label('product_url'),
    History.asin,
    func.avg(History.price).label('price_avg'),
    func.min(History.price).label('price_min'),
    func.max(History.price).label('price_max'),
    func.sum(History.quantity).label('quantity_count'),
    func.count(History.asin).label('buy_count')). \
    group_by(History.asin). \
    order_by(func.sum(History.quantity).desc()).\
    filter(History.asin == ASIN).\
    all()

# print(type(all_data))
# print(all_data[0])
product_name = all_data[0][0]
product_li = product_name.split(',')
print('商品名は', product_li[0])

price_avg = all_data[0][3]
print('平均価格は', price_avg)

price_min = all_data[0][4]
print('最低価格は', price_min)

buy_count = all_data[0][7]
print('購入回数は', buy_count)

product_url = all_data[0][1]
url_li = product_url.split(',')
print('URL一覧')
for url in url_li:
    print(url)
# print(all_data)
# print(all_data.product_url)
# print(all_data.asin_count)
# print(all_data.quantity_count)




