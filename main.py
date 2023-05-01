from models.gspread import Gspread


def main():
    gs = Gspread()
    gs.get_pharaoh()


if __name__ == "__main__":
    main()
