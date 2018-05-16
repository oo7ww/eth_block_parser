#!/usr/bin/python
#transfer tx file in mongodb to csv

from pymongo import MongoClient
import csv

client = MongoClient('localhost', 27017)
tx = client.eth_block.transaction_cl

FIELDS = ['hash', 'from', 'to', 'gas', 'gasPrice', 'type']

csv_file = open('tx.csv', 'w')

dict_writer = csv.DictWriter(csv_file, fieldnames = FIELDS)
dict_writer.writeheader()

dict_writer.writerows(tx.find())

csv_file.close()
client.close()

