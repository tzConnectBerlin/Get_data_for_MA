import json
import pandas as pd
import requests
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

def get_tzstats_block(level):
    url = f"https://api.tzstats.com/explorer/block/{level}"
    r = smart_fetch(url)
    return r.json()

def cycle_start_and_end_level(cycle_index):
	url = f"https://api.tzkt.io/v1/cycles/{cycle_index}"
	r = smart_fetch(url)
	json_obj = r.json()
	return json_obj['firstLevel'], json_obj['lastLevel']

def get_block_data(start_level, stop_level):
    lst_cycle = []
    lst_level = []
    lst_priority = []
    lst_validations = []
    lst_fees = []
    lst_n_tx = []
    lst_n_ops_applied = []
    lst_n_ops_failed = []
    lst_gas_used = []
    lst_timestamp = []
    for level in tqdm(range(start_level, stop_level + 1)):
        tzstats_block = get_tzstats_block(level)

        lst_cycle.append(tzstats_block['cycle'])
        assert level == tzstats_block['height']
        lst_level.append(tzstats_block['height'])
        lst_priority.append(tzstats_block['round'])
        lst_validations.append(tzstats_block['n_endorsed_slots'])
        lst_fees.append(tzstats_block['fee'])
        lst_n_tx.append(tzstats_block['n_tx'])
        lst_n_ops_applied.append(tzstats_block['n_ops_applied'])
        lst_n_ops_failed.append(tzstats_block['n_ops_failed'])
        lst_gas_used.append(tzstats_block['gas_used'])
        lst_timestamp.append(tzstats_block['time'])
        data = {'Cycle': lst_cycle, 'Level': lst_level, 
                'Priority': lst_priority, 'Validations': lst_validations,
                'Fees': lst_fees, 'Transactions': lst_n_tx,
                'Ops_Applied': lst_n_ops_applied, 'Ops_Failed': lst_n_ops_failed,
                'Gas_Used': lst_gas_used, 'Block_Timestamp': lst_timestamp}
    return data

if __name__ == '__main__':
    start_level, stop_level = cycle_start_and_end_level(cycle_index=0)
    block_data = get_block_data(start_level, stop_level)
    data_df = pd.DataFrame(block_data)
    path = f"tzstats_block_{start_level}_{stop_level}.csv"
    data_df.to_csv(path, index=False)
