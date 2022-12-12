from sqlalchemy import Column, Integer, String, DateTime, Sequence, Float
from settings.setting import ENGINE, Base
import sys


class History(Base):
    """
    UserModel
    """
    __tablename__ = 'history'
    id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
    date = Column(DateTime)
    shop = Column(String(100))
    product_name = Column(String(255))
    price = Column(Integer)
    shipping_cost = Column(Integer)
    quantity = Column(Integer)
    asin = Column(String(20))
    # amazon_link = Column(String(255))
    # keepa_link = Column(String(255))
    unit_price = Column(Integer)
    rate = Column(Float)
    product_url = Column(String(255))
    # bought_by = Column(String(20))


def main(args):
    Base.metadata.create_all(bind=ENGINE)


if __name__ == "__main__":
    main(sys.argv)
