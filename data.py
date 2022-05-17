import json
import pandas as pd
import requests
import datetime
from tqdm import tqdm
import time


def smart_fetch(uri):
    while True:
        start = time.time()
        try:
            result = requests.get(uri)
            if (result.status_code // 100 == 4):  # 4xx we treat as permanent
                print("Error " + str(result.status_code) +
                      " while fetching " + uri)
                return None
            return result
        except Exception as e:
            print("Error while fetching " + uri +
                  "; Retrying indefinitely...\n" + str(e))
        # let's not get throttled lol :PPPP
        remaining = 30 - (time.time() - start)
        if (remaining > 0):
            time.sleep(remaining)

def load_head_block_data(path):
	with open(path, 'r') as fh:
		head_block = fh.read()

	json_obj = json.loads(head_block)

	return json_obj


def get_num_transactions_in_block(level):
	url = f"https://api.tzkt.io/v1/operations/transactions/count/?level={level}"
	r = smart_fetch(url)
	return r.json()

def get_tzkt_block(level):
	url = f"https://api.tzkt.io/v1/blocks/{level}"
	r = smart_fetch(url)
	return r.json()

def cycle_start_and_end_level(cycle_index):
	url = f"https://api.tzkt.io/v1/cycles/{cycle_index}"
	r = smart_fetch(url)
	json_obj = r.json()
	return json_obj['firstLevel'], json_obj['lastLevel']


def get_tzkt_block_transactions(level):
	offset=0
	list_json = []

	while True:
		path = f"https://api.tzkt.io/v1/operations/transactions/?level={level}&limit=100&offset={offset}"
		response_string = requests.get(path).text
		list_obj = json.loads(response_string)
		
		if len(list_obj) == 0:
			break
		
		list_json += list_obj
		offset += 100
	
	return list_json

def get_block_data(start_level, stop_level):
	lst_cycle = []
	lst_level = []
	lst_priority = []
	lst_validations = []
	lst_fees = []
	lst_numTransactions = []
	lst_timestamp = []
	for level in tqdm(range(start_level, stop_level + 1)):
		tzkt_block = get_tzkt_block(level)
		nr_of_transactions = get_num_transactions_in_block(level)

		lst_cycle.append(tzkt_block['cycle'])
		assert level == tzkt_block['level']
		lst_level.append(tzkt_block['level'])
		lst_priority.append(tzkt_block['priority'])
		lst_validations.append(tzkt_block['validations'])
		lst_fees.append(tzkt_block['fees'])
		lst_timestamp.append(tzkt_block['timestamp'])
		lst_numTransactions.append(nr_of_transactions)
	data = {'Cycle': lst_cycle, 'Level': lst_level, 
			'Priority': lst_priority, 'NumValidations': lst_validations, 
			'Fees': lst_fees, 'NumTransactions': lst_numTransactions, 
			'BlockTimestamp': lst_timestamp}
	return data




d1 = datetime.datetime.strptime('2022-03-23T09:53:14Z','%Y-%m-%dT%H:%M:%SZ')
d2 = datetime.datetime.strptime('2022-03-23T09:54:54Z','%Y-%m-%dT%H:%M:%SZ')

if __name__ == '__main__':
	start_level, stop_level = cycle_start_and_end_level(cycle_index=1)
	block_data = get_block_data(start_level, stop_level)
	data_df = pd.DataFrame(block_data)
	path = f"block_{start_level}_to_{stop_level}.csv"
	data_df.to_csv(path, idex=False)
