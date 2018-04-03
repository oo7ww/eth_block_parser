#!/usr/bin/python
# -*- coding: utf-8 -*-
# parse a block and write it to text file 
# insert into mongodb
from web3 import Web3, IPCProvider
from pymongo import MongoClient

#convert an AttributeDict to normal dict
# 1 block to dict
def A_Dict(block):
    my_dict = dict()
    for key, value in block.items():
        my_dict[str(key)] = value
    return my_dict 

# 2 transaction to dict 
def T_Dict(trans_list):
    trans_dict = dict()
    for i in range(len(trans_list)):
        hash_val = trans_list[i]['hash']
        trans_dict[hash_val] = trans_list[i]
    return trans_dict

#connect to the geth node
web3 = Web3(IPCProvider("/root/.ethereum/geth.ipc"))

#create a mongoClient
client = MongoClient('localhost', 27017)

#create a database
db = client['eth_block']

collection = db['block_cl']
count = 0

with open('/root/eth_block_parser/block_data.txt','w') as f:
   for count in range(46147,46157):
       block = web3.eth.getBlock(count)
       n_dict = A_Dict(block)
       #test 
       print(str(n_dict['hash']))
       a = str(count) + '\n'
       f.write(a)
       
       blk_num = block.number
       trans_num = web3.eth.getBlockTransactionCount(blk_num)
       n_dict['transaction_count'] = trans_num
       print(trans_num)
       trans_str = 'this block has ' + str(trans_num) + 'transactions\n'
       f.write(trans_str)
       trans_list = []
       # get transactions and convert to dict
       for i in range(0,trans_num):
           trans_temp = web3.eth.getTransactionFromBlock(blk_num, i)
           trans_list.append(A_Dict(trans_temp))
      # for key, value in n_dict.items():
      #     t = str(key) + ' ' + str(value) + '\n'
      #     print(t)
      
       trans_dict = T_Dict(trans_list)
       n_dict['transactions_dict'] = trans_dict
       for key, value in block.items():
           b = str(key) + ' ' + str(value) + '\n'
           f.write(b)
       e = collection.insert_one(n_dict)
       print(str(e))  


print('process over!')


