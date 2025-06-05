import ib_insync
from ib_insync import IB

from sysdata.data_blob import dataBlob
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
from sysbrokers.IB.ib_instruments_data import ibFuturesInstrumentData
from sysbrokers.IB.ib_contracts import (
    get_ib_contract_with_specific_expiry,
    resolve_unique_contract_from_ibcontract_list
)
from sysbrokers.IB.ib_connection_defaults import ib_defaults
from sysobjects.contracts import futuresContract

from .ServerData import ServerData

# data = dataBlob()
# data.add_class_object(ibFuturesContractData)
# data.add_class_object(ibFuturesInstrumentData)
import ib_insync
import logging



async def connect_ib() -> IB:
    """
    Connects to IB asynchronously and returns the connected IB object.
    """
    ib_insync.util.logToConsole(logging.INFO)

    ib = IB()
    
    # Get connection parameters.
    client_id = ServerData.data._get_next_client_id_for_ib()
    ipaddress, port, _ = ib_defaults()
    
    # Connect asynchronously.
    await ib.connectAsync(host=ipaddress, port=port, clientId=client_id)
    ServerData.ib = ib
    
async def disconnect_ib():
    
    ServerData.ib.disconnect()

async def test_ib_connection():
    """Test the IB connection by requesting the current server time."""
    try:
        ib = ServerData.ib
        server_time = await ib.reqCurrentTimeAsync()
        print(f"Connected to IB. Server Time: {server_time}")
        return True
    except Exception as e:
        print(f"IB connection test failed: {e}")
        return False

async def get_contract_details(futures_contract: futuresContract):
    
    """
    Given an already-connected IB instance, create the contract
    and request its details asynchronously.
    """



    ib = ServerData.ib
    
    broker_futures_contract_data = ServerData.broker_futures_contract_data
    
    # Retrieve the contract object with IB metadata.
    futures_contract_with_ib_data = broker_futures_contract_data._get_contract_object_with_IB_metadata(
        futures_contract
    )
    
    # Extract instrument and contract date from the returned object.
    futures_instrument_with_ib_data = futures_contract_with_ib_data.instrument
    contract_date = futures_contract_with_ib_data.contract_date
    
    # Get the IB contract using a helper.
    ib_contract = get_ib_contract_with_specific_expiry(
        futures_instrument_with_ib_data, contract_date
    )
    ib_contract.includeExpired = False
    
    # Use the asynchronous API to request contract details.
    contract_details_list = await ib.reqContractDetailsAsync(ib_contract)

    ibcontract_list = [
        contract_details.contract for contract_details in contract_details_list 
    ]

    
    try:
        resolved_contract = resolve_unique_contract_from_ibcontract_list(
            ibcontract_list=ibcontract_list,
            futures_instrument_with_ib_data=futures_instrument_with_ib_data,
        )
    except Exception as exception:
        print(
            "%s could not resolve contracts: %s"
            % (str(futures_instrument_with_ib_data), exception.args[0])
        )
        return futures_contract.key, None        

            # Find the corresponding ContractDetails object in contract_details_list
    resolved_contract_details = next(
        (cd for cd in contract_details_list if cd.contract == resolved_contract), 
        None
    )
    if resolved_contract_details is None:
        raise Exception("No matching ContractDetails found for the resolved contract.")

    # Return the key and the resolved ContractDetails object.
    return futures_contract.key, resolved_contract_details
    