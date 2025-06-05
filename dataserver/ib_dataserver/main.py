import asyncio
import time
import atexit

from .core.ib_functions import (
    connect_ib,
    test_ib_connection,
    disconnect_ib,
)
from .core.instrument_data import (
    refresh_contract_cache
)
from .core.server_functions import start_server
from .core.ticker_event_functions import (
    subscription_loop,
    flush_all_cache,
)
from .core.ServerData import ServerData
from kyle_tests.ib_monitor.ib_monitor import check_ib_valid_time


async def _ib_loop() -> None:
    """
    Main async loop for:
    1) Checking if IB is open
    2) Connecting to IB
    3) Refreshing contract cache
    4) Subscribing to market data every minute
    5) Handling reconnect logic when the IB connection drops
    """
    # Store the running loop on ServerData for other modules to reference if needed
    ServerData.loop = asyncio.get_running_loop()

    while True:
        # 1) Check IB hours
        if not check_ib_valid_time():
            print("[Server] IB is closed, retrying connection in one minute.")
            await asyncio.sleep(60)
            continue

        # 2) Try connecting to IB
        try:
            await connect_ib()

            try:
                # 3) Force-refresh contract cache
                await refresh_contract_cache(force_refresh=True)

                # 4) Enter the subscription loop
                while True:
                    # Before each cycle, verify IB connection is still alive
                    if not await test_ib_connection():
                        print("[Server] IB connection failed. Cleaning up and retrying...")
                        # Reset any in-memory caches
                        ServerData.ticker_dict = {}
                        ServerData.contract_cache = {}
                        await disconnect_ib()
                        break

                    # Fetch new market data / handle subscription events
                    await subscription_loop()

                    # Wait one minute before next subscription cycle
                    await asyncio.sleep(60)

            finally:
                # Ensure we always disconnect from IB before exiting this inner block
                await disconnect_ib()

        except Exception as e:
            print(f"[Server] IB connection attempt failed: {e}")
            # If any exception occurs (e.g. network error), pause before retrying
            await asyncio.sleep(60)


def main() -> None:
    """
    Entry point when running as a script or via console_scripts.

    1) Register atexit handler to flush caches on shutdown.
    2) Start any synchronous server components.
    3) Launch the async IB loop.
    """
    # Ensure caches are cleared on process exit
    atexit.register(flush_all_cache)

    # Start any HTTP / WebSocket / RPC servers defined in core.server_functions
    start_server()

    # Run the async IB loop until the process is killed
    asyncio.run(_ib_loop())


if __name__ == "__main__":
    main()
