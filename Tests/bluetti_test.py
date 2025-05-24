import asyncio
import logging
import os
import sys
import signal # Ensure signal is imported
from typing import List, Dict, Any 
from bleak import BleakScanner 
from bleak.backends.device import BLEDevice as BleakBLEDevice 
from bleak.backends.scanner import AdvertisementData

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from devices.bluetti_ble import BluettiDeviceWrapper 

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TestBluetti")

running_wrappers: List[BluettiDeviceWrapper] = []
stop_all_event = asyncio.Event() # This event will signal all tasks to stop

def signal_handler(sig, frame):
    logger.info(f"Received signal {sig}, setting stop_all_event to initiate graceful shutdown...")
    # This event is awaited by the main_test_loop, so setting it will allow main_test_loop to exit its wait
    # and proceed to the finally block for cleanup.
    stop_all_event.set()

async def my_data_handler(data_packet: Dict[str, Any]):
    logger.info(f"--- Data Received by Test Handler for {data_packet.get('device_id')} ---")
    logger.info(f"Source: {data_packet.get('source')}")
    logger.info(f"Model: {data_packet.get('model')}")
    data = data_packet.get('data', {})
    logger.info(f"  Battery SOC: {data.get('total_battery_percent')}")
    logger.info(f"  Total PV Power: {data.get('dc_input_power')}")   
    logger.info(f"  AC Output Power: {data.get('ac_output_power')}") 
    logger.info(f"------------------------------------")

async def main_test_loop():
    global stop_all_event, running_wrappers
    logger.info("Starting Bluetti Wrapper Test Program...")

    ac180_mac = os.getenv("BLUETTI_AC180_MAC", "").upper()
    eb3a_mac = os.getenv("BLUETTI_EB3A_MAC", "").upper()
    
    target_mac_map: Dict[str, str] = {} 
    if ac180_mac:
        target_mac_map[ac180_mac] = "AC180"
    if eb3a_mac:
        target_mac_map[eb3a_mac] = "EB3A"

    if not target_mac_map:
        logger.error("No Bluetti MAC addresses configured. Exiting.")
        return

    logger.info(f"Performing a single discovery scan (20s) for all devices...")
    
    # Store BLEDevice and AdvertisementData objects found
    # Key: MAC Address (str), Value: tuple(BLEDevice, AdvertisementData)
    found_devices_cache: Dict[str, tuple[BleakBLEDevice, AdvertisementData]] = {}
    
    try:
        # Use BleakScanner.discover() once to get all devices and their advertisement data
        # This is generally more reliable for getting advertisement data than repeated find_device_by_address
        # The returned value is a dictionary: {mac_address_str: (BLEDevice_obj, AdvertisementData_obj)}
        discovered_devices_with_adv = await BleakScanner.discover(timeout=20.0, return_adv=True)

        if discovered_devices_with_adv:
            for mac_str_discovered, (device_obj, adv_data_obj) in discovered_devices_with_adv.items():
                mac_upper = mac_str_discovered.upper()
                if mac_upper in target_mac_map: # Check if this discovered device is one we're looking for
                    logger.info(f"Target device {target_mac_map[mac_upper]} ({mac_upper}) found in scan. Name: {device_obj.name or adv_data_obj.local_name}")
                    found_devices_cache[mac_upper] = (device_obj, adv_data_obj)
        else:
            logger.warning("Initial broad scan using BleakScanner.discover() found no devices.")

    except Exception as e:
        logger.error(f"Error during initial BLE discovery using BleakScanner.discover(): {e}", exc_info=True)
        return

    if not found_devices_cache:
        logger.error(f"None of the target devices ({list(target_mac_map.keys())}) were found with advertisement data. Exiting.")
        return
    
    logger.info(f"Scan complete. Found and cached info for: {list(found_devices_cache.keys())}")

    # --- Initialize and Start Wrappers for Found Devices ---
    for mac_address, device_label in target_mac_map.items():
        if mac_address in found_devices_cache:
            ble_device, ad_data = found_devices_cache[mac_address]
            logger.info(f"Initializing Bluetti Wrapper for {device_label} ({mac_address})")
            
            wrapper = BluettiDeviceWrapper(
                mac_address=mac_address,
                discovered_ble_device=ble_device, # Pass the BLEDevice object
                advertisement_data=ad_data,       # Pass the AdvertisementData object
                data_callback=my_data_handler,
                polling_interval_seconds=25,
                connection_timeout_seconds=30 
            )
            await wrapper.start() 
            running_wrappers.append(wrapper)
        else:
            logger.warning(f"Target device {device_label} ({mac_address}) was NOT found in the scan results. Skipping.")

    if not running_wrappers:
        logger.error("No device wrappers were successfully started. Exiting.")
        return

    try:
        logger.info("All configured devices started. Test loop running. Press Ctrl+C to stop.")
        await stop_all_event.wait() # Wait until signal_handler sets this event
    except asyncio.CancelledError:
        logger.info("Main test loop task was cancelled.")
    finally:
        logger.info("Main test loop ending. Initiating shutdown of all device wrappers...")
        # Stop wrappers in reverse order or concurrently
        shutdown_tasks = [wrapper.stop() for wrapper in running_wrappers]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True) # Gather ensures all stops are attempted
        logger.info("All device wrappers signaled to stop and awaited.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    try:
        asyncio.run(main_test_loop())
    except KeyboardInterrupt: # This might still be hit if Ctrl+C happens before asyncio.run() or during its teardown
        logger.info("Test program terminated by KeyboardInterrupt outside main async loop.")
    finally:
        logger.info("Test program fully exited.")