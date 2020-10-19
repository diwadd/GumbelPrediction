import requests
import sqlite3
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime

class StockDataRetriever:

    def __init__(self, web_page_addresses):
        self.web_page_addresses = web_page_addresses
        self.data = {}

    def process_row(self, full_row):

        stock_full_names = full_row.find_all(id="f10")
        stock_short_name_and_prices = full_row.find_all(id="f13")

        current_stock = None
        for ssnap in stock_short_name_and_prices:

            # Get stock short symbol
            short_name = ssnap.find("a")
            if short_name is not None:
                current_stock = short_name.contents[0]

                if current_stock in self.data:
                    continue
                else:
                    self.data[current_stock] = {"Name": stock_full_names[0].contents[0]}
            
            # Get stock change
            outer = ssnap.find(id=f"aq_{current_stock.lower()}_m1")
            if outer is not None:

                inner = outer.find(id="c1")
                if inner is not None:
                    self.data[current_stock]["Change"] = inner.contents[0]
                
                inner = outer.find(id="c2")
                if inner is not None:
                    self.data[current_stock]["Change"] = inner.contents[0]

                inner = outer.find(id="c3")
                if inner is not None:
                    self.data[current_stock]["Change"] = inner.contents[0]


    def retrieve_single_web_page_address(self, web_page_address):
        
        page = requests.get(web_page_address)
        soup = BeautifulSoup(page.content, 'html.parser')

        # Assuming stooq
        table = soup.find_all(class_="fth1")

        index = 0
        while True:
            full_row = table[0].find_all(id=f"r_{index}")
            if len(full_row) == 0:
                break

            self.process_row(full_row[0])
            index += 1



    def retrieve_stock_data(self):
        
        for web_page_address in self.web_page_addresses:
            self.retrieve_single_web_page_address(web_page_address)

class DataBaseInteraction:
    def __init__(self, db_file_name):
        self.db_file_name = db_file_name
        self.stock_changes_table_name = "StockChanges"
        self.connection = None
        self.cursor = None

    def check_if_table_exists(self, table_name):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=':table_name';", {"table_name": table_name})
        return self.cursor.fetchone()

    def process_data_and_add_records(self, data):

        def convert_change_to_up_down(change):
            up_down = float(change.replace("%", "").replace("+",""))
            if up_down > 0.0:
                up_down = 1
            elif up_down < 0.0:
                up_down = -1
            elif up_down == 0.0:
                up_down = 0.0
            return up_down

        todays_date = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        for k, v in data.items():
            print(f"{k} {v}")
            stock_name = v["Name"]
            change = v["Change"]
            up_down = convert_change_to_up_down(change)

            self.add_record(todays_date, stock_name, up_down, change)


    def add_record(self, todays_date, stock_name, up_down, change):
        self.cursor.execute(f"INSERT INTO {self.stock_changes_table_name} VALUES ('{todays_date}', '{stock_name}', '{up_down}', '{change}')")
        self.connection.commit()

    def __enter__(self):

        self.connection = sqlite3.connect(self.db_file_name)
        self.cursor = self.connection.cursor()

        try:
            self.cursor.execute(f'''CREATE TABLE {self.stock_changes_table_name} 
                                    (Date text, Symbol text, UpDown int, Change real, 
                                    PRIMARY KEY (Date, Symbol, UpDown, Change) ON CONFLICT IGNORE)''')
        except sqlite3.OperationalError as err:
            print(err)
        
        return self

    def __exit__(self, type, value, traceback):
        print("In __exit__, cleaning and exiting!")
        self.connection.close()


def update_data_dictionary(d, stock_change_data):

    today = datetime.today().strftime("%Y-%m-%d")
    for k, v in stock_change_data.items():
 
        n = v["Name"]
        e = [today, v["Change"]]
        if n not in d:
            d[n] = []
            d[n].append( e )
        else:
            d[n].append( e )

if __name__ == "__main__":

    web_page_addresses = ["https://stooq.pl/t/?i=582"]

    sdr = StockDataRetriever(web_page_addresses)
    sdr.retrieve_stock_data()

    print(sdr.data)


    
    # stock_data_file_name = "stock_change_data.json"
    # if os.path.isfile(stock_data_file_name) == False:
    #     print(f"File {stock_data_file_name} does not exist!")
    #     with open(stock_data_file_name, 'w') as f:
    #         d = {}
    #         update_data_dictionary(d, sdr.data)
    #         json.dump(d, f, ensure_ascii=False, indent=4)
    # else:
    #     print(f"File {stock_data_file_name} does exist!")
    #     with open(stock_data_file_name, 'r') as f:
    #         d = json.loads(f.read())
    #         print(f"d: {d}")
        
    #     with open(stock_data_file_name, 'w') as f:
    #         update_data_dictionary(d, sdr.data)
    #         json.dump(d, f, ensure_ascii=False, indent=4)

    with DataBaseInteraction("stock_changes.db") as dbi:
        print("In context manager")
        print(dbi.check_if_table_exists("StockChanges"))
        
        dbi.process_data_and_add_records(sdr.data)