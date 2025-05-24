# logger_project/devices/bluetti_ble.py

import asyncio
import logging
import re # Added for regex
from typing import Callable, Dict, Any, Optional, Awaitable, List

from bleak import BleakScanner, AdvertisementData
from bleak.backends.device import BLEDevice as BleakBLEDevice
from bleak.exc import BleakDeviceNotFoundError, BleakError

# --- Imports from your vendored bluetti-mqtt files ---
try:
    # Assuming 'devices.core_bluetti' is how Python will see it from the project root
    from .core_bluetti.bluetooth.client import BluetoothClient, ClientState
    from .core_bluetti.bluetooth.encryption import is_device_using_encryption
    
    # Import all known device types that build_specific_bluetti_instance might return
    from .core_bluetti.devices.bluetti_device import BluettiDevice
    from .core_bluetti.devices.v2_device import V2Device
    from .core_bluetti.devices.eb3a import EB3A

    from .core_bluetti.commands import ReadHoldingRegisters, DeviceCommand # DeviceCommand also needed by BluetoothClient
    from .core_bluetti.bluetooth.exc import BadConnectionError, ModbusError, ParseError
except ImportError as e:
    print(f"CRITICAL: Failed to import vendored bluetti-mqtt components in bluetti_ble.py: {e}")
    print("Please ensure bluetti-mqtt core files are correctly placed and import paths are updated.")
    raise

logger = logging.getLogger(__name__)

# Regex from bluetti-mqtt/bluetooth/__init__.py
# This will likely need to be updated if bluetti-mqtt adds more device prefixes
DEVICE_NAME_RE = re.compile(r'^(AC180|AC200M|AC300|AC500|AC60|EP500P|EP500|EP600|EB3A)(\d*)$')
# Made the SN part (\d+) optional (\d*) as some names might just be "AC180" from some scanners initially.
# The original build_device uses match[2] for SN.

def build_specific_bluetti_instance(address: str, device_ble_name: str) -> Optional[BluettiDevice]:
    """
    Factory function to create a device-specific BluettiDevice instance
    based on its advertised name. Adapted from bluetti-mqtt.
    """
    match = DEVICE_NAME_RE.match(device_ble_name)
    if not match:
        logger.warning(f"Bluetti: Device name '{device_ble_name}' does not match known patterns.")
        return None

    model_prefix = match.group(1)
    serial_number_suffix = match.group(2) if match.group(2) else "UnknownSN" # Handle if no digits found

    logger.debug(f"Bluetti build_specific: Model Prefix='{model_prefix}', SN Suffix='{serial_number_suffix}' from name '{device_ble_name}'")

    # Map model prefix to the corresponding class and type string
    # This requires that V2Device, EB3A, AC200M etc. classes are imported in this file's scope.
    if model_prefix == 'AC180':
        # AC180 uses the V2Device profile in bluetti-mqtt
        return V2Device(address, serial_number_suffix, 'AC180') 
    elif model_prefix == 'EB3A':
        return EB3A(address, serial_number_suffix) # EB3A constructor in bluetti-mqtt takes (address, sn)
    # Add elif clauses for other device types from the original build_device as you vendor their class files:
    # elif model_prefix == 'AC200M':
    #     return AC200M(address, serial_number_suffix)
    # elif model_prefix == 'AC300':
    #     return AC300(address, serial_number_suffix)
    # ... and so on for AC500, AC60, EP500, EP500P, EP600

    else:
        logger.error(f"Bluetti: Unsupported model prefix '{model_prefix}' derived from name '{device_ble_name}'.")
        return None


class BluettiDeviceWrapper:
    def __init__(self,
                 mac_address: str,
                 discovered_ble_device: BleakBLEDevice, 
                 advertisement_data: AdvertisementData,
                 data_callback: Callable[[Dict[str, Any]], Awaitable[None]],
                 polling_interval_seconds: int = 30,
                 connection_timeout_seconds: int = 20): # connection_timeout is now for BleakClient.connect
        self.mac_address = mac_address.upper()
        self.discovered_ble_device = discovered_ble_device
        self.advertisement_data = advertisement_data
        self.data_callback = data_callback
        self.polling_interval_seconds = polling_interval_seconds
        # self.connection_timeout_seconds is now used by BleakClient, not scanner

        self._ble_client: Optional[BluetoothClient] = None
        self._specific_device_instance: Optional[BluettiDevice] = None
        self._is_encrypted: Optional[bool] = None
        self._device_ble_name: Optional[str] = None # Will get from discovered_ble_device
        
        self._main_task: Optional[asyncio.Task] = None
        self._client_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._is_initialized = False 
        self._is_running = False

    async def _initialize_components(self) -> bool: # Renamed from _discover_and_init_ble_components
        """Initializes components using pre-discovered device info."""
        if self._is_initialized:
            return True

        logger.info(f"Bluetti Wrapper ({self.mac_address}): Initializing components...")

        if not self.discovered_ble_device.name:
            self._device_ble_name = self.advertisement_data.local_name
            if not self._device_ble_name:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): No name available. Cannot determine model.")
                return False
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Using name from advertisement: '{self._device_ble_name}'.")
        else:
            self._device_ble_name = self.discovered_ble_device.name
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Using device name: '{self._device_ble_name}'.")

        self._is_encrypted = is_device_using_encryption(self.advertisement_data.manufacturer_data)
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Uses encryption: {self._is_encrypted}.")

        self._specific_device_instance = build_specific_bluetti_instance(self.mac_address, self._device_ble_name)
        if not self._specific_device_instance:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Could not create specific device instance for '{self._device_ble_name}'.")
            return False
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Initialized as specific device type '{type(self._specific_device_instance).__name__}'.")

        # Pass the BleakBLEDevice object to BleakClient if its constructor can take it,
        # otherwise, it will use the MAC address.
        # BleakClient(address_or_ble_device, ...)
        self._ble_client = BluetoothClient(self.discovered_ble_device, self._is_encrypted) # Pass BLEDevice object
        # If BluetoothClient only takes address string:
        # self._ble_client = BluetoothClient(self.mac_address, self._is_encrypted)
        # For this to work, BluetoothClient.__init__ needs to accept a BleakBLEDevice object
        # or you continue passing self.mac_address
        # Let's check bluetti-mqtt's BluetoothClient: it takes `address: str`. So we stick to MAC.
        self._ble_client = BluetoothClient(self.mac_address, self._is_encrypted)


        logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient instantiated.")
        self._is_initialized = True
        return True

    async def _poll_device_data_once(self) -> Optional[Dict[str, Any]]:
        if not self._is_initialized or not self._ble_client or not self._specific_device_instance:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Attempted to poll but not initialized.")
            return None
        
        if not self._ble_client.is_ready:
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient not ready. Skipping poll.")
            return None

        all_polled_data = {}
        logger.debug(f"Bluetti Wrapper ({self.mac_address}): Starting data poll cycle for '{self._device_ble_name}'.")

        commands_to_poll: List[DeviceCommand] = self._specific_device_instance.polling_commands
        if not commands_to_poll:
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): No polling commands defined for device type '{type(self._specific_device_instance).__name__}'.")
            return None # Or empty dict if that's preferred for no commands

        for command in commands_to_poll:
            if self._stop_event.is_set():
                logger.info(f"Bluetti Wrapper ({self.mac_address}): Stop event set during poll cycle.")
                return None # Abort poll cycle

            if not isinstance(command, ReadHoldingRegisters):
                logger.debug(f"Bluetti Wrapper ({self.mac_address}): Skipping non-ReadHoldingRegisters command: {command!r}")
                continue

            logger.debug(f"Bluetti Wrapper ({self.mac_address}): Sending command {command!r}")
            try:
                # BluetoothClient.perform returns a future. We await it.
                response_bytes_future = await self._ble_client.perform(command)
                # Add a timeout specifically for awaiting the command's result from the client's internal future
                raw_response_bytes = await asyncio.wait_for(response_bytes_future, timeout=self._ble_client.RESPONSE_TIMEOUT + 2) # Use client's timeout + buffer

                if raw_response_bytes:
                    data_body = command.parse_response(raw_response_bytes)
                    parsed_fields = self._specific_device_instance.parse(command.starting_address, data_body)
                    all_polled_data.update(parsed_fields)
                    logger.debug(f"Bluetti Wrapper ({self.mac_address}): Parsed for {command.starting_address}: {parsed_fields}")
                else:
                    logger.warning(f"Bluetti Wrapper ({self.mac_address}): No response (None) received for command {command!r}")

            except asyncio.TimeoutError:
                logger.warning(f"Bluetti Wrapper ({self.mac_address}): Timeout waiting for response to command {command!r} from BluetoothClient.")
            except (ModbusError, ParseError) as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Modbus/Parse error for command {command!r}: {e}")
            except BadConnectionError as e: # This is a custom exception from bluetti-mqtt
                logger.error(f"Bluetti Wrapper ({self.mac_address}): BadConnectionError during command {command!r}: {e}. Client will attempt reconnect via its run loop.")
                return None # End this poll cycle, let client.run() handle it
            except BleakError as e: # Catch Bleak specific errors
                logger.error(f"Bluetti Wrapper ({self.mac_address}): BleakError during command {command!r}: {e}. Client will attempt reconnect.")
                return None # End this poll cycle
            except Exception as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Unexpected error polling command {command!r}: {e}", exc_info=True)
                return None # End this poll cycle

        logger.debug(f"Bluetti Wrapper ({self.mac_address}): Poll cycle completed. Data: {all_polled_data if all_polled_data else 'None'}")
        return all_polled_data if all_polled_data else None


    async def run_polling_loop(self):
        """Main async loop for this device: initializes components, then polls periodically."""
        self._is_running = True
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Starting polling loop.")

        # Components are now initialized using pre-discovered info
        if not await self._initialize_components():
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Failed component initialization. Stopping polling loop.")
            self._is_running = False
            return
        
        # Start the BluetoothClient's internal run loop as a separate, managed task
        if not self._ble_client: # Should have been set by _initialize_components
            logger.critical(f"Bluetti Wrapper ({self.mac_address}): _ble_client is None after initialization! Cannot proceed.")
            self._is_running = False
            return
            
        self._client_task = asyncio.create_task(self._ble_client.run(), name=f"BluettiClientRun-{self.mac_address}")
        
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Waiting for Bluetooth client to establish connection and readiness...")
        # Give the client a moment to establish initial connection and readiness
        # We need a more robust way to wait for the client to be truly ready,
        # especially if encryption handshake is involved.
        # BluetoothClient.is_ready should reflect this after its run() has progressed.
        initial_wait_cycles = 0
        max_initial_wait_cycles = int(self.connection_timeout_seconds / 2) + 5 # e.g., 15 cycles if timeout is 20s

        while not self._ble_client.is_ready:
            if self._client_task.done() or initial_wait_cycles >= max_initial_wait_cycles:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task ended or timed out waiting for readiness. Last state: {self._ble_client.state.name}")
                await self._shutdown_client_task()
                self._is_running = False
                return
            logger.debug(f"Bluetti Wrapper ({self.mac_address}): Client not ready yet (State: {self._ble_client.state.name}), waiting... ({initial_wait_cycles+1}/{max_initial_wait_cycles})")
            await asyncio.sleep(2) # Check every 2 seconds
            initial_wait_cycles += 1
        
        logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient is ready (State: {self._ble_client.state.name}). Starting periodic polling.")

        try:
            while not self._stop_event.is_set():
                if self._client_task.done(): 
                    try:
                        await self._client_task 
                    except Exception as e:
                        logger.error(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task terminated with error: {e}", exc_info=True)
                    else:
                        logger.error(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task terminated unexpectedly. Stopping polling.")
                    break 

                if not self._ble_client.is_ready:
                    logger.warning(f"Bluetti Wrapper ({self.mac_address}): Client no longer ready (State: {self._ble_client.state.name}). Waiting for reconnect by client...")
                    await asyncio.sleep(self.polling_interval_seconds / 2) 
                    continue

                logger.debug(f"Bluetti Wrapper ({self.mac_address}): Initiating data poll.")
                polled_data = await self._poll_device_data_once()

                if polled_data:
                    await self.data_callback({
                        'source': 'bluetti',
                        'device_id': self.mac_address,
                        'model': self._specific_device_instance.type if self._specific_device_instance else 'Unknown',
                        'data': polled_data
                    })
                elif polled_data is None and self._ble_client.is_ready:
                    logger.warning(f"Bluetti Wrapper ({self.mac_address}): Poll returned no data, but client is ready. Check device or commands.")

                try:
                    logger.debug(f"Bluetti Wrapper ({self.mac_address}): Next poll in {self.polling_interval_seconds} seconds.")
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.polling_interval_seconds)
                except asyncio.TimeoutError:
                    pass 
        
        except asyncio.CancelledError:
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Polling loop explicitly cancelled.")
        except Exception as e:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Unhandled exception in polling loop: {e}", exc_info=True)
        finally:
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Polling loop attempting to cleanup and finish.")
            await self._shutdown_client_task()
            self._is_running = False
            # self._is_initialized = False # Keep true unless re-discovery is explicitly desired on next start

    async def _shutdown_client_task(self):
        """Helper to gracefully shut down the BluetoothClient task."""
        if self._client_task and not self._client_task.done():
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Cancelling BluetoothClient task...")
            self._client_task.cancel()
            try:
                await self._client_task
            except asyncio.CancelledError:
                logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task successfully cancelled.")
            except Exception as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Error during BluetoothClient task cancellation/shutdown: {e}", exc_info=True)
        
        # The BluetoothClient.run() has its own finally block to disconnect.
        # We rely on that. If direct control is needed, add it here.
        logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task shutdown process completed.")


    async def start(self):
        """Starts the main polling loop for this device if not already running."""
        if self._is_running:
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): Start called but already running or main task exists.")
            return
        if self._main_task and not self._main_task.done():
             logger.warning(f"Bluetti Wrapper ({self.mac_address}): Main task already exists and is not done. Not starting new one.")
             return

        self._stop_event.clear()
        self._is_running = True # Set before creating task
        self._main_task = asyncio.create_task(self.run_polling_loop(), name=f"BluettiPollLoop-{self.mac_address}")
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Main polling task created and started.")

    async def stop(self):
        """Signals the polling loop to stop and waits for it to clean up."""
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Stop called.")
        if not self._is_running and not self._main_task: # If neither flag/task indicates running
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Was not running or no main task to stop.")
            return

        self._stop_event.set() # Signal all loops to stop

        if self._main_task and not self._main_task.done():
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Waiting for main polling task to complete...")
            try:
                # Give enough time for one poll cycle + client shutdown attempts
                await asyncio.wait_for(self._main_task, timeout=self.polling_interval_seconds + 15) 
            except asyncio.TimeoutError:
                logger.warning(f"Bluetti Wrapper ({self.mac_address}): Timeout waiting for main task to stop gracefully. Attempting cancellation.")
                self._main_task.cancel()
                try:
                    await self._main_task # Await the cancelled task to process cancellation
                except asyncio.CancelledError:
                    logger.info(f"Bluetti Wrapper ({self.mac_address}): Main task successfully cancelled after timeout.")
                except Exception as e: # Catch other exceptions during forced shutdown
                    logger.error(f"Bluetti Wrapper ({self.mac_address}): Exception after cancelling main task: {e}", exc_info=True)
            except Exception as e: # Catch other exceptions while waiting for main task
                 logger.error(f"Bluetti Wrapper ({self.mac_address}): Exception while waiting for main task to complete: {e}", exc_info=True)
        
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Stop processing finished.")
        self._main_task = None # Clear the task reference
        self._is_running = False # Mark as not running
        # self._is_initialized can remain true, ready for a restart, unless explicitly reset