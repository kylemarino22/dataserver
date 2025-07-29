from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Manager
import threading
import time

from .ServerData import ServerData 
from .instrument_data import is_market_open_sync
from sysobjects.contracts import futuresContract

class ServerManager(BaseManager):
    pass

# Functions to manage queues dynamically
def create_queue(stream, subscriber_id):
    """Create a queue for a new subscriber under a specific stream."""

    queues = ServerData.queues
    manager = ServerData.manager

    print(f"[Server] create_queue called with stream={stream}, subscriber_id={subscriber_id}")


    if stream not in queues:
        queues[stream] = manager.dict()  # Initialize stream-level dict
    
    if subscriber_id not in queues[stream]:
        queues[stream][subscriber_id] = manager.Queue()
        print(f"[Server] Created queue for {subscriber_id} on stream '{stream}'")
    
    return ServerData.queues[stream][subscriber_id]

def remove_queue(stream, subscriber_id):
    """Unsubscribe and remove queue if no subscribers remain."""
    
    queues = ServerData.queues

    if stream in queues and subscriber_id in queues[stream]:
        del queues[stream][subscriber_id]
        print(f"[Server] {subscriber_id} unsubscribed from '{stream}'")

        # Remove stream if no subscribers remain
        if not queues[stream]:
            del queues[stream]
            print(f"[Server] Removed stream '{stream}' (no subscribers left)")

def is_market_open_for_key(key):
    """Get market status for key"""
    # params = key.split("/")
    # futures_contract = futuresContract(*params) 
    return is_market_open_sync(key)


def monitor_queues():
    queues = ServerData.queues
    
    while True:
        print(f"[Server] Active streams: {list(queues.keys())}")
        for s, subs in queues.items():
            print(f"[Server]  └─ {s} → subscribers: {list(subs.keys())}")
        time.sleep(60*5)

def start_server():

    # Register functions for remote access
    ServerManager.register('get_tracked_keys',
                        callable=lambda: list(ServerData.ticker_dict.keys()))
    ServerManager.register('create_queue', callable=create_queue)
    ServerManager.register('remove_queue', callable=remove_queue)
    ServerManager.register('is_market_open_for_key', callable=is_market_open_for_key)

    # Start the manager server
    manager_server = ServerManager(address=('127.0.0.1', 50000), authkey=b'secret')
    

    print("[Server] Data Publisher is running...")
    threading.Thread(target=manager_server.get_server().serve_forever, daemon=True).start()
    threading.Thread(target=monitor_queues, daemon=True).start()



def unsubscribe_from_market_data(key):
    """Unsubscribes from an instrument and replaces it with the next available one."""
    ticker_dict = ServerData.ticker_dict

    if key in ticker_dict.keys():
        print(f"[INFO] Unsubscribing from {key} due to market closure.")

        ServerData.ib.cancelMktData(ticker_dict[key][1])
        del ticker_dict[key]