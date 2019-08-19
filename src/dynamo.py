"""Dynamo Example"""

import csv
from pprint import pprint
import boto3
import threading

session = boto3.Session(profile_name="default")
dynamodb = session.resource('dynamodb', region_name='us-west-2')

def dynamo_create_table(table_name, key_schema, attribute_definitions):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            }
        )
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        print("table created")
        return True
    except Exception as e:
        print(e)
        return False



def import_data_multithreaded(filepath):
    print("opening file: %s" % filepath)
    collection_name = filepath.split("/")[-1].split(".")[0]
    print("creating collection: %s" % collection_name)

    with open(filepath) as file:
        reader = csv.reader(file, delimiter=",")

        header = False
        for row in reader:
            if not header:
                header = [h for h in row]

                dynamo_create_table(
                    collection_name,
                    [
                        {
                            'AttributeName': header[0],
                            'KeyType': 'HASH'
                        }
                    ],
                    [
                        {
                            'AttributeName': header[0],
                            'AttributeType': 'S'
                        },
                    ],
                )
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
                    dynamodb.Table(collection_name).put_item(
                        Item=data
                    )
                except Exception as e:
                    print(e)

                # pprint(data)
                # break





if __name__ == "__main__":
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


    # print(dynamodb.Table("product").scan()["Items"])


    print("goodbye")

