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

#There are 3 types of ETH transaction: simple transaction, contract-creating transaction, contract-executing transaction
#simple transaction: just do the from to staff, transactionReceipt.contractAddress is Null
#contract-creating transaction: same to its name, to value is Null and transactionReceipt.contractAddress is not Null
#contract-executing transaction: identified by the contract-address dict
#identify a transaction's type
def Type_id(transaction, transReceipt)
    tran = dict()
    tran = transaction
    if tran['to'] == Null:
      tran['type'] = 'contact-creating'
      address = transReceipt['contractAddress']
      global contract_addr
      contract_addr[address] = 'true'
    elif tran['to'] in contract_addr:
      tran['type'] = 'contract-executing'
    else:
      tran['type'] = 'contract-none'
    return tran

#connect to the geth node
web3 = Web3(IPCProvider("/root/.ethereum/geth.ipc"))

#create a mongoClient
client = MongoClient('localhost', 27017)

#create a database
db = client['eth_block']

#block and transaction collections
block_collection = db['block_cl']
transaction_collection = db['transaction_cl']
# collection for contract address
contract_collection = db['contract_cl'] 

# global dict for contract address
contract_addr = dict()

count = 0

with open('/root/eth_block_parser/block_data.txt','w') as f:
   for count in range(46147,46157):
       #get a block
       block = web3.eth.getBlock(count)
       #turn a block's AttributeDict to normal dict 
       n_dict = A_Dict(block)
       #test and ignore
       print(str(n_dict['hash']))
       a = str(count) + '\n'
       f.write(a)
       
       blk_num = block.number
       trans_num = web3.eth.getBlockTransactionCount(blk_num)
       #add the transactions count to the normal dict of block
       n_dict['transaction_count'] = trans_num
       
       #test and ignore
       print(trans_num)
       trans_str = 'this block has ' + str(trans_num) + 'transactions\n'
       f.write(trans_str)

       #new a transaction list
       trans_list = []
       
       # get transactions and convert to dict
       for i in range(0,trans_num):
           trans_temp = A_Dict(web3.eth.getTransactionFromBlock(blk_num, i))
           trans_hash = trans_temp['hash']
           trans_Receipt = A_Dict(web3.eth.getTransactionReceipt(trans_hash))
           trans_todb = Type_id(trans_temp, trans_hash)
           #insert a transaction to MongoDB
           flag = transaction_collection.insert_one(trans_todb)
           test_trans = 'insert a transaction ' + str(flag) + '\n'  
           print(test_trans)
           trans_list.append(A_Dict(trans_temp))
      # for key, value in n_dict.items():
      #     t = str(key) + ' ' + str(value) + '\n'
      #     print(t)
      
      #add the transactions dict to a block dict
       trans_dict = T_Dict(trans_list)
      #n_dict['transactions_dict'] = trans_dict
       
       #test and ignore
       for key, value in block.items():
           b = str(key) + ' ' + str(value) + '\n'
           f.write(b)

       #insert a block to Mongodb    
       e = block_collection.insert_one(n_dict)
       #check the insert op
       print(str(e))  
       #save contract_address to db
       test_contract = contract_collection.insert_one(contract_addr) 
       print(str(test_contract))
       
print('process over!')


