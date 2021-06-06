from GooseClass import Goose
from datetime import datetime
import os.path

class Testapp:
    # today = datetime.date.today().strftime("%Y%m%d")
    now = datetime.now()
    today = now.strftime("%m%d%Y_%H_%M_%S")
    file_path = f"data/ticks_{today}.csv"
    # file_path = f"data/ticks_{ticker}_{today}.csv"
    if not os.path.exists("data"):
        os.makedirs("data")
    fd = open(file_path, "w")

    # self.fd.write(f"{time},{tickType},{price},{size}\n")

    def first(self):
        numbers = [0, 1, 2, 5, 8, 15]
        for i in numbers:
            print(Goose.sum_to(i))
            self.fd.write(f"{Goose.sum_to(i)}\n")

    def second(self):
        numbers = [20,25]
        for i in numbers:
            print(Goose.sum_to(i))
            self.fd.write(f"{Goose.sum_to(i)}\n")

def main():
    app = Testapp()
    app.first()
    app.second()
    app.first()

if __name__ == "__main__":
    main()