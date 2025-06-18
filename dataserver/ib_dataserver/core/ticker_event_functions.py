import asyncio
import numpy as np
from datetime import datetime, timezone
import os
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import uuid

# import pyarrow.compute as pc


from .ServerData import ServerData
from .instrument_data import (
    load_instr_list, is_market_open, get_ib_contract, is_market_open_sync
)
from .server_functions import unsubscribe_from_market_data
from sysobjects.contracts import futuresContract
from sysexecution.tick_data import oneTick
from ib_insync import Ticker

from collections import defaultdict




# Directory to store Parquet files
PARQUET_PATH = os.path.expanduser("/mnt/nas/parquet_tick_data/")
os.makedirs(PARQUET_PATH, exist_ok=True)

MAX_SUBSCRIPTIONS = 199  # Max allowed instruments to subscribe at a time

async def subscription_loop():
    """
    Continuously checks if there are fewer than MAX_SUBSCRIPTIONS active.
    If so, it goes through the instrument list and subscribes to those that are open.
    If no markets are available, it waits 5 minutes before trying again.
    This function should be run in a dedicated thread.
    """

    ticker_dict = ServerData.ticker_dict
    data_contract_obj = ServerData.data_contracts
    # broker_obj = ServerData.broker_futures_contract_data

    # Load the full list of instruments (used for both queue and priority ordering).
    instrument_priority_list = load_instr_list()
    # Create a mapping from instrument_code to its priority (lower index means higher priority)
    instrument_priority_map = {code: i for i, code in enumerate(instrument_priority_list)}
    
    # Copy the list for processing subscriptions (we'll pop from this queue)
    instrument_queue = instrument_priority_list.copy()

    # Build a mapping: instrument_code -> list of ticker_dict keys
    tracked_codes_to_keys = defaultdict(list)
    for key, (futures_contract, _) in ticker_dict.items():
        tracked_codes_to_keys[futures_contract.instrument_code].append(key)

    # Build the ticker_keys_priority_sorted list from already subscribed instruments,
    # sorting by the instrument's priority.
    ticker_keys_priority_sorted = list(ticker_dict.keys())
    ticker_keys_priority_sorted.sort(key=lambda key: instrument_priority_map.get(ticker_dict[key][0].instrument_code, float('inf')))

    # valid_subscriptions = len(ticker_keys_priority_sorted)

    valid_subscriptions = 0

    print("[INFO] Entering subscription loop.")

    while valid_subscriptions < MAX_SUBSCRIPTIONS:
        # If the instrument list is empty or no instruments are available, wait.
        if not instrument_queue:
            print("[INFO] No available instruments left. Exiting loop")
            break
            # await asyncio.sleep(5 * 60)
            # instrument_queue = load_instr_list().copy()
            # continue

        instrument_code = instrument_queue.pop(0)

        # Skip if we're already tracking this instrument.
        if instrument_code in tracked_codes_to_keys:
            valid_subscriptions += 1
            continue

        # Create a futuresContract and check if we're already subscribed.
        price_contract_id = data_contract_obj.get_priced_contract_id(instrument_code)
        futures_contract = futuresContract(instrument_code, price_contract_id)
        key = futures_contract.key
        if key in ticker_dict:
            continue

        # Check if the market is open for this contract.
        if not await is_market_open(futures_contract):
            print(f"[INFO] Market closed for {instrument_code}; skipping...")
            continue

        # If we have reached MAX_SUBSCRIPTIONS, unsubscribe from the lowest priority instrument.
        if len(ticker_keys_priority_sorted) >= MAX_SUBSCRIPTIONS:
            # Remove the lowest priority key (the last in our sorted list)
            lowest_priority_key = ticker_keys_priority_sorted.pop()
            unsubscribe_from_market_data(lowest_priority_key)
            # Also remove the unsubscribed key from ticker_dict so that our state remains consistent.
            ticker_dict.pop(lowest_priority_key, None)
            valid_subscriptions -= 1

        # Get the IB contract and ticker.
        ib_contract = await get_ib_contract(futures_contract)
        ib_ticker = get_ib_ticker(ib_contract)

        # Subscribe by saving the subscription in ticker_dict and wiring up the event.
        ticker_dict[key] = (futures_contract, ib_ticker)
        ib_ticker.updateEvent += lambda ticker, code=key: on_price_update(ticker, code)
        print(f"[INFO] Subscribed to {key}, {valid_subscriptions}")

        # Update tracked_codes_to_keys mapping.
        tracked_codes_to_keys[instrument_code].append(key)

        # Insert the new key into ticker_keys_priority_sorted in the correct position.
        new_priority = instrument_priority_map.get(instrument_code, float('inf'))
        insertion_index = 0
        for i, existing_key in enumerate(ticker_keys_priority_sorted):
            existing_code = ticker_dict[existing_key][0].instrument_code
            existing_priority = instrument_priority_map.get(existing_code, float('inf'))
            if new_priority < existing_priority:
                insertion_index = i
                break
        else:
            insertion_index = len(ticker_keys_priority_sorted)
        ticker_keys_priority_sorted.insert(insertion_index, key)

        valid_subscriptions += 1

            
def get_ib_ticker(ib_contract):
    
    ib = ServerData.ib
    ib.reqMarketDataType(1)
    ib.reqMktData(ib_contract, "", False, False)
    return ib.ticker(ib_contract)

def on_price_update(ticker: Ticker, key: str):
    """Callback function when new market data arrives."""
    data_tick = oneTick(ticker.bid, ticker.ask, ticker.bidSize, ticker.askSize)
    ticker_dict = ServerData.ticker_dict

    # Check for NaN or negative values
    if np.any(np.isnan([data_tick.bid_price, data_tick.ask_price, data_tick.bid_size, data_tick.ask_size])) or \
       any(x < 0 for x in [data_tick.bid_price, data_tick.ask_price, data_tick.bid_size, data_tick.ask_size]):
        # Look up the contract for this key
        futures_contract, _ = ticker_dict.get(key, (None, None))
        if futures_contract:
            # Use the cached trading hours to check if it is currently okay to trade
            if not is_market_open_sync(futures_contract):
                print(data_tick.bid_price, data_tick.ask_price, data_tick.bid_size, data_tick.ask_size)
                print(f"[INFO] Unsubscribing from {key} due to market closure (bad data & trading hours).")
                unsubscribe_from_market_data(key)
                return
        print(f"[WARNING] {key} | Received bad data, but market appears open. Ignoring bad tick.")
        return

    # print(f"[EVENT] {key} | Bid: {data_tick.bid_price} | Ask: {data_tick.ask_price}")
    asyncio.run_coroutine_threadsafe(handle_new_data(key, data_tick), ServerData.loop)

async def handle_new_data(key, data_tick: oneTick):
    """Handles new data: caches it & notifies all subscribers."""
    
    # Cache the data
    await cache_price_data(key, data_tick)
    
    queues = ServerData.queues

    print(f"[Program 1] Publishing '{data_tick}' to {len(queues[key])} subscribers")

    # Iterate over all subscribers of this stream and send data
    for subscriber_id, queue in queues[key].items():
        queue.put(data_tick)  # Push data to each subscriber's queue


async def cache_price_data(key, data_tick):
    """Caches price data and writes after 100 records."""
    
    parquet_cache = ServerData.parquet_cache
    
    if key not in parquet_cache:
        parquet_cache[key] = []

    # Append the current tick with a UTC timestamp
    parquet_cache[key].append([datetime.now(timezone.utc), *data_tick])

    # Write to Parquet only when cache reaches 100 records
    if len(parquet_cache[key]) >= 1000:
        block = parquet_cache[key]
        start_time = block[0][0]
        end_time = block[-1][0]
        time_delta = (end_time - start_time).total_seconds()

        if time_delta > 0:
            avg_ticks_sec = len(block) / time_delta
        else:
            avg_ticks_sec = float('inf')  # In case the time difference is 0
        
        print(f"[Info] Writing parquet data for {key}. Average ticks/sec: {avg_ticks_sec:.2f}")
        await save_to_parquet(key)


async def save_to_parquet(key):
    """
    Writes cached price data into a single daily Parquet file directly under the contract directory.

    Folder layout:
        PARQUET_PATH/{instrument_code}/contract={contract}/
            {YYYYMMDD}.parquet

    We write only the 5 tick columns here:
        timestamp (as UTC datetime),
        bid_price (float64),
        ask_price (float64),
        bid_size  (int64),
        ask_size  (int64)

    The partition values "contract" and "trading_date" are inferred at read time.
    """
    # ----------------------------------------------------------------------------
    # Block 1: Parse key and retrieve cached rows.
    # ----------------------------------------------------------------------------
    try:
        instrument_code, contract = key.split("/")
    except ValueError:
        raise ValueError("Key must be in the format 'instrument_code/contract'")

    parquet_cache = ServerData.parquet_cache
    rows = parquet_cache.get(key, [])

    if not rows:
        # Nothing to write
        return

    # ----------------------------------------------------------------------------
    # Block 2: Derive trading_date (YYYYMMDD) from the first row's datetime.
    # ----------------------------------------------------------------------------
    first_dt: datetime = rows[0][0]
    if first_dt.tzinfo is None:
        # Ensure it's timezone‐aware UTC; assume naive dt is UTC
        first_dt = first_dt.replace(tzinfo=timezone.utc)
    trading_date_str = first_dt.strftime("%Y%m%d")

    # ----------------------------------------------------------------------------
    # Block 3: Build contract directory and ensure it exists.
    # ----------------------------------------------------------------------------
    contract_dir = Path(PARQUET_PATH) / instrument_code / f"contract={contract}"
    contract_dir.mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------------------------
    # Block 4: Define the single daily filename.
    # ----------------------------------------------------------------------------
    daily_file = contract_dir / f"{trading_date_str}.parquet"
    tmp_file   = contract_dir / f"{trading_date_str}.parquet.tmp"

    # ----------------------------------------------------------------------------
    # Block 5: Build a PyArrow Table from the cached rows.
    # ----------------------------------------------------------------------------
    timestamps = [r[0] for r in rows]
    bid_prices = [r[1] for r in rows]
    ask_prices = [r[2] for r in rows]
    bid_sizes  = [r[3] for r in rows]
    ask_sizes  = [r[4] for r in rows]

    new_table = pa.Table.from_arrays(
        [
            pa.array(timestamps, type=pa.timestamp("ns", tz="UTC")),
            pa.array(bid_prices, type=pa.float64()),
            pa.array(ask_prices, type=pa.float64()),
            pa.array(bid_sizes,  type=pa.int64()),
            pa.array(ask_sizes,  type=pa.int64()),
        ],
        names=[
            "timestamp",
            "bid_price",
            "ask_price",
            "bid_size",
            "ask_size",
        ],
    )

    # ----------------------------------------------------------------------------
    # Block 6: Read existing daily file (if any) and merge with new data.
    # ----------------------------------------------------------------------------
    if daily_file.exists():
        try:
            # Read only the columns present in new_table to drop any extras (e.g. 'contract')
            existing_table = pq.read_table(
                str(daily_file),
                columns=new_table.schema.names
            )
            # print(f"  existing rows: {existing_table.num_rows}")
            combined_table = pa.concat_tables([existing_table, new_table])
        except Exception as err:
            print(f"  failed to read existing parquet: {err!r}")
            combined_table = new_table
    else:
        combined_table = new_table

    # ----------------------------------------------------------------------------
    # Block 7: Write the combined table atomically (via a temporary file).
    # ----------------------------------------------------------------------------
    try:
        pq.write_table(combined_table, tmp_file)
        os.replace(tmp_file, daily_file)  # atomic rename
    except Exception as e:
        # Clean up partial temp file on failure
        if tmp_file.exists():
            try:
                os.remove(tmp_file)
            except OSError:
                pass
        raise RuntimeError(f"Error writing daily Parquet file to {daily_file}: {e}")

    # ----------------------------------------------------------------------------
    # Block 8: Clear the in‐memory cache for this key.
    # ----------------------------------------------------------------------------
    parquet_cache[key] = []




def flush_all_cache():
    print("[INFO] Flushing all cached data to Parquet before exit...")

    parquet_cache = ServerData.parquet_cache

    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    
    try:
        # Iterate over a static list of keys to avoid modification during iteration
        for key in list(parquet_cache.keys()):
            if parquet_cache[key]:
                try:
                    new_loop.run_until_complete(save_to_parquet(key))
                    print(f"[INFO] Flushed cache for {key}")
                except Exception as e:
                    print(f"[ERROR] Failed to flush cache for {key}: {e}")
    finally:
        new_loop.close()

        
async def periodic_flush(interval_seconds: int = 60) -> None:
    """
    Background task that wakes up every `interval_seconds` and flushes any
    non-empty caches by calling save_to_parquet(key) for each key.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        now = datetime.now(timezone.utc)
        for key, block in list(ServerData.parquet_cache.items()):
            if block:
                try:
                    await save_to_parquet(key)
                    print(f"[INFO] Periodic flush at {now.isoformat()} for {key}")
                except Exception as e:
                    print(f"[ERROR] Periodic flush failed for {key}: {e}")