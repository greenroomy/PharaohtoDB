from models.gspread import Gspread
from models.history import *
from settings.setting import session
from settings.setting import ENGINE


def main():
    gs = Gspread()
    gs.get_pharaoh()


if __name__ == "__main__":
    main()
