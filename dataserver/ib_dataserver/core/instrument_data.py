import pandas as pd
import os
import pickle
import time
import asyncio

from .ServerData import ServerData
from .ib_functions import get_contract_details
from ib_insync import IB
from sysobjects.contracts import futuresContract
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
from sysbrokers.IB.ib_trading_hours import get_unadjusted_trading_hours_from_contract_details


# Cache file location
CACHE_FILE = os.path.join(os.path.expanduser("~"), ".trading_hours_cache.pkl")

# Save one subscription in case we need to access ikbr through rob's old code
# instrument_queue = []  # Stores instruments in priority order
# active_subscriptions = {}  # Keeps track of active instruments {key: contractObj}


def load_instr_list():
    """Loads the instrument list from CSV once and stores it in a global variable."""
    # global _instr_list
    instr_list = ServerData.instr_list
    if instr_list is None:
        csv = pd.read_csv("/home/kyle/pysystemtrade/kyle_tests/data_engine/config/config_acc1.csv")
        instr_list = [instr for instr in csv['Instrument'] if not str(instr).startswith('#')]
    return instr_list.copy()  # Return a copy to avoid modifying the original list

# Get function to only load contract cache once
def get_contract_cache():
    
    if ServerData.contract_cache is None:
        ServerData.contract_cache = load_contract_cache()

    return ServerData.contract_cache

def load_contract_cache():
    """Loads the contract cache from a single pickle file."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
            return cache
        except Exception as e:
            print(f"[WARNING] Could not load contract cache: {e}")
            return {}
    else:
        return {}

def save_contract_cache(cache):
    """Saves the contract cache to the pickle file."""
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)
    except Exception as e:
        print(f"[ERROR] Could not save contract cache: {e}")


async def refresh_contract_cache(force_refresh=False):

    instr_list = load_instr_list()
    data_contract_obj = ServerData.data_contracts
    
    contract_cache = get_contract_cache()
    current_time = time.time()
    
    # Set up coroutines to get contract data
    coroutines = []
    for instrument_code in instr_list:
        
        # Check if instrument_code is in contract_cache and if timestamp within the last day

        priced_contract_id = data_contract_obj.get_priced_contract_id(instrument_code) 
        futures_contract = futuresContract(instrument_code, priced_contract_id)
        
        if not force_refresh:
            key = futures_contract.key
            if key in contract_cache:
                entry = contract_cache[key]
                timestamp = entry.get("timestamp", 0)
                # If cached data is less than one day old, use it
                if current_time - timestamp < 86400:
                    continue

        coroutines.append(get_contract_details(futures_contract))
        

    if len(coroutines) == 0:
        print(f"No need to write to cache")
        return 

    results = await asyncio.gather(*coroutines)

    for key, contract_details in results:

        contract_cache[key] = {"timestamp": current_time, "contract_details": contract_details}
        
    save_contract_cache(contract_cache)
    print(f"Wrote to cache for {len(instr_list)} instruments")
    
def get_cached_contract_details_sync(futures_contract: futuresContract):
    
    contract_cache = get_contract_cache()

    key = futures_contract.key

    current_time = time.time()

    if key in contract_cache:
        entry = contract_cache[key]
        timestamp = entry.get("timestamp", 0)
        # If cached data is less than one day old, use it
        if current_time - timestamp < 86400:
            return entry['contract_details']   

    return None
    
async def get_cached_contract_details(futures_contract: futuresContract):
    
    contract_cache = get_contract_cache()

    key = futures_contract.key

    current_time = time.time()

    if key in contract_cache:
        entry = contract_cache[key]
        timestamp = entry.get("timestamp", 0)
        # If cached data is less than one day old, use it
        if current_time - timestamp < 86400:
            return entry['contract_details']            

    # If key not in contract_cache or expired, get new
    key, contract_details = await get_contract_details(futures_contract)

    contract_cache[key] = {"timestamp": current_time, "contract_details": contract_details}
    save_contract_cache(contract_cache)
    return contract_details
    

async def get_ib_contract(futures_contract: futuresContract):

    contract_details = await get_cached_contract_details(futures_contract)

    return contract_details.contract

    

async def is_market_open(futures_contract: futuresContract):
    
    contract_details = await get_cached_contract_details(futures_contract)

    if contract_details is None:
        return False

    trading_hours = get_unadjusted_trading_hours_from_contract_details(contract_details)

    return trading_hours.okay_to_trade_now()

    
def is_market_open_sync(futures_contract: futuresContract):
    
    contract_details = get_cached_contract_details_sync(futures_contract)

    # Unsubscribe if details out of date
    if contract_details is None:
        return False

    trading_hours = get_unadjusted_trading_hours_from_contract_details(contract_details)

    return trading_hours.okay_to_trade_now()

    