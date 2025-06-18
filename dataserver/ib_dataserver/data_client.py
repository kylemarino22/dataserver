
from sysdata.data_blob import dataBlob
from sysproduction.data.broker import dataBroker
from sysobjects.contracts import futuresContract

from sysbrokers.IB.ib_futures_contract_price_data import ibFuturesContractPriceData
import time

from kyle_tests.data_engine.TrackedDataClient import TrackedDataClient

if __name__ == "__main__":
    # data = dataBlob()
    # data_broker = dataBroker(data)

    instrument_code = "US10"
    contract_id = "20250900"
    futures_contract = futuresContract(instrument_code, contract_id)

    ib_obj = ibFuturesContractPriceData(None, None)

    trackedDataClient = TrackedDataClient()
    trackedKeys = trackedDataClient.get_tracked_keys()
    trackedKeys = trackedKeys._callmethod("copy")
    print(trackedKeys)
    print(f"Subscribed to {len(trackedKeys)} keys")


    is_market_open = trackedDataClient.is_market_open_for_key(futures_contract.key)
    
    print(f"Is market open for {futures_contract.key}? -> {is_market_open}")

    ticker_object = ib_obj.get_ticker_object_for_contract(futures_contract)

    tick = ticker_object.wait_for_valid_bid_and_ask_and_return_current_tick()
     
    del ticker_object

    print(tick)
    