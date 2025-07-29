
from multiprocessing import Queue, Manager

from sysdata.data_blob import dataBlob
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
from sysbrokers.IB.ib_instruments_data import ibFuturesInstrumentData
from sysproduction.data.contracts import dataContracts

class ServerData:
    
    # Shared storage for subscriber queues
    manager = Manager()
    queues = manager.dict()  # { "stream_name": { "subscriber_id": Queue() } }
    ticker_dict = {}
    instr_list = None
    
    data = dataBlob(ib_conn=None)

    data.add_class_object(ibFuturesContractData)
    data.add_class_object(ibFuturesInstrumentData)

    broker_futures_contract_data = ibFuturesContractData(None, data)

    data_contracts = dataContracts(data)
    ib = None

    contract_cache = None

    parquet_cache = {}

    async_loop = None