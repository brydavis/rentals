"""Lesson 05 Mongo Example"""

import csv
from pprint import pprint
from pymongo import MongoClient

import threading

mongo = MongoClient("mongodb://localhost:27018")
db = mongo["norton"]

def import_data(data_dir, *files):
    for filepath in files:
        print("opening file: %s" % filepath)
        collection_name = filepath.split(".")[0]
        print("creating collection: %s" % collection_name)

        with open("/".join([data_dir, filepath])) as file:
            reader = csv.reader(file, delimiter=",")

            header = False
            table = db[collection_name]

            for row in reader:
                if not header:
                    header = [h for h in row]
                else:
                    data = {
                        header[column]:value 
                        for column, value in enumerate(row)
                    }

                    # {'Credit_limit': '237',
                    #  'Email_address': 'Jessy@myra.net',
                    #  'Home_address': '337 Eichmann Locks',
                    #  'Id': 'C000000',
                    #  'Last_name': 'Shanahan',
                    #  'Name': 'Rickey',
                    #  'Phone_number': '1-615-598-8649 x975',
                    #  'Status': 'Active'

                    try:
                        table.insert_one(data)
                    except Exception as e:
                        print(e)

                    # pprint(data)
                    # break



def import_data_multithreaded(filepath):
    print("opening file: %s" % filepath)
    collection_name = filepath.split(".")[0]
    print("creating collection: %s" % collection_name)

    with open(filepath) as file:
        reader = csv.reader(file, delimiter=",")

        header = False
        table = db[collection_name]

        for row in reader:
            if not header:
                header = [h for h in row]
            else:
                data = {
                    header[column]:value 
                    for column, value in enumerate(row)
                }

                # {'Credit_limit': '237',
                #  'Email_address': 'Jessy@myra.net',
                #  'Home_address': '337 Eichmann Locks',
                #  'Id': 'C000000',
                #  'Last_name': 'Shanahan',
                #  'Name': 'Rickey',
                #  'Phone_number': '1-615-598-8649 x975',
                #  'Status': 'Active'

                try:
                    table.insert_one(data)
                except Exception as e:
                    print(e)

                # pprint(data)
                # break





if __name__ == "__main__":
    db["customer"].drop()
    db["rental"].drop()
    db["product"].drop()
    # import_data("data", "customer.csv", "product.csv", "rental.csv")

    files = [
        "data/product.csv",
        "data/rental.csv",
        "data/customer.csv",
    ]

    threads = []
    for filepath in files:
        thread = threading.Thread(
            target=import_data_multithreaded,
            args=(filepath,)
        )
        thread.start()
        threads.append(thread)

    # blocking
    for thread in threads:
        thread.join()

    print("goodbye")

