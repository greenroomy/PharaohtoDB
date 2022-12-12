from models.gspread import Gspread
from models.history import *
from settings.setting import session
from settings.setting import ENGINE


def main():
    gs = Gspread()
    gs.get_pharaoh()
    # res = gs.get_pharaoh()
    # res.to_sql(
    #     name='pharaoh',
    #     con=ENGINE,
    #     if_exists='append',
    #     index=False,
    #     chunksize=1000,
    #     method='multi'
    # )


if __name__ == "__main__":
    main()
