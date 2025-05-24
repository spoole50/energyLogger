# logger_project/devices/bluetti_ble.py

import asyncio
import logging
from typing import Callable, Dict, Any, Optional, Awaitable, List

from bleak import BleakScanner, AdvertisementData
from bleak.backends.device import BLEDevice as BleakBLEDevice # Alias to avoid name clashes
from bleak.exc import BleakDeviceNotFoundError, BleakError

# --- Imports from your vendored bluetti-mqtt files ---
# Adjust these paths based on your exact vendoring structure.
# Example: if 'core_bluetti' is a sub-package in 'devices'
try:
    from .core_bluetti.bluetooth.client import BluetoothClient, ClientState
    from .core_bluetti.bluetooth.encryption import is_device_using_encryption
    # The build_device function is complex due to its dynamic imports of device types.
    # For simplicity in this wrapper, we'll assume we might need to replicate parts of its logic
    # or ensure it's importable and works in the new context.
    # For now, let's assume we'll get the device name and then manually pick the right class.
    # If build_device can be made to work easily, that's better.
    from .core_bluetti.devices.v2_device import V2Device # Example for AC180
    from .core_bluetti.devices.eb3a import EB3A # Example for EB3A
    # Add other device types as needed
    from .core_bluetti.devices.bluetti_device import BluettiDevice # Base class
    from .core_bluetti.commands import ReadHoldingRegisters, DeviceCommand
    from .core_bluetti.bluetooth.exc import BadConnectionError, ModbusError, ParseError
except ImportError as e:
    # This helps catch issues if vendoring/imports are not set up correctly
    print(f"CRITICAL: Failed to import vendored bluetti-mqtt components: {e}")
    print("Please ensure bluetti-mqtt core files are correctly placed and import paths are updated.")
    raise

logger = logging.getLogger(__name__)

# Simplified build_device logic (adapt as needed or import the original if possible)
# This needs access to all specific device classes.
def build_specific_bluetti_instance(address: str, device_ble_name: str) -> Optional[BluettiDevice]:
    # Simplified from bluetti-mqtt's DEVICE_NAME_RE and build_device
    # This is a placeholder; the original build_device is more robust.
    # You would need to ensure V2Device, EB3A, AC200M etc. are imported here or passed in.
    if 'AC180' in device_ble_name:
        # The 'sn' part (match[2]) from original build_device might be part of the name
        # For now, passing a placeholder or extracting it if format is consistent
        sn_part = device_ble_name.replace('AC180', '').strip() # Simplistic SN extraction
        return V2Device(address, sn_part if sn_part else "UnknownSN", 'AC180')
    elif 'EB3A' in device_ble_name:
        sn_part = device_ble_name.replace('EB3A', '').strip()
        return EB3A(address, sn_part if sn_part else "UnknownSN") # Assuming EB3A class exists and takes these args
    # Add other elif for other models you expect from bluetti-mqtt build_device
    logger.warning(f"Bluetti: No specific device class found for name '{device_ble_name}'. Falling back to generic BluettiDevice (limited functionality).")
    # Fallback or raise error if you want to be strict
    # return BluettiDevice(address, "UnknownType", "UnknownSN") # Or return None
    return None


class BluettiDeviceWrapper:
    def __init__(self,
                 mac_address: str,
                 data_callback: Callable[[Dict[str, Any]], Awaitable[None]],
                 polling_interval_seconds: int = 30,
                 connection_timeout_seconds: int = 20):
        self.mac_address = mac_address.upper()
        self.data_callback = data_callback
        self.polling_interval_seconds = polling_interval_seconds
        self.connection_timeout_seconds = connection_timeout_seconds # Timeout for initial connect

        self._ble_client: Optional[BluetoothClient] = None
        self._specific_device_instance: Optional[BluettiDevice] = None
        self._is_encrypted: Optional[bool] = None
        self._device_ble_name: Optional[str] = None
        
        self._main_task: Optional[asyncio.Task] = None
        self._client_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._is_initialized = False
        self._is_running = False

    async def _discover_and_init_ble_components(self) -> bool:
        """Scans for the device, determines encryption, and initializes BLE client and device parser."""
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Discovering...")
        device_adv_tuple: Optional[tuple[BleakBLEDevice, AdvertisementData]] = None
        try:
            # Using find_device_by_address with return_adv=True
            device_adv_tuple = await BleakScanner.find_device_by_address(
                self.mac_address, timeout=self.connection_timeout_seconds, return_adv=True
            )
        except BleakDeviceNotFoundError:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Device not found during scan.")
            return False
        except BleakError as e:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): BleakError during discovery: {e}")
            return False
        except Exception as e:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Unexpected error during discovery: {e}", exc_info=True)
            return False

        if not device_adv_tuple:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Device not found after scan attempt.")
            return False
        
        bleak_device, advertisement_data = device_adv_tuple

        if not bleak_device.name:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Found but has no name. Cannot determine model.")
            return False

        self._device_ble_name = bleak_device.name
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Found '{self._device_ble_name}'.")

        # Determine encryption status
        self._is_encrypted = is_device_using_encryption(advertisement_data.manufacturer_data)
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Uses encryption: {self._is_encrypted}.")

        # Instantiate the specific Bluetti device parser (V2Device, EB3A, etc.)
        self._specific_device_instance = build_specific_bluetti_instance(self.mac_address, self._device_ble_name)
        if not self._specific_device_instance:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Could not create specific device instance for '{self._device_ble_name}'.")
            return False
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Initialized as '{type(self._specific_device_instance).__name__}'.")

        # Instantiate the BluetoothClient (from bluetti-mqtt)
        self._ble_client = BluetoothClient(self.mac_address, self._is_encrypted)
        logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient instantiated.")
        return True

    async def _poll_device_data_once(self) -> Optional[Dict[str, Any]]:
        """Performs a single polling cycle to fetch all configured data from the device."""
        if not self._ble_client or not self._specific_device_instance:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Client or device instance not initialized for polling.")
            return None
        
        if not self._ble_client.is_ready: # is_ready from BluetoothClient
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient not ready. Skipping poll.")
            return None # Rely on BluetoothClient's run() loop to reconnect/ready

        all_polled_data = {}
        logger.debug(f"Bluetti Wrapper ({self.mac_address}): Starting data poll cycle.")

        commands_to_poll: List[DeviceCommand] = self._specific_device_instance.polling_commands
        if not commands_to_poll:
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): No polling commands defined for device type '{type(self._specific_device_instance).__name__}'.")
            return None

        for command in commands_to_poll:
            if self._stop_event.is_set(): break # Check for stop signal between commands
            if not isinstance(command, ReadHoldingRegisters): # We only handle reading registers
                continue

            logger.debug(f"Bluetti Wrapper ({self.mac_address}): Sending command {command!r}")
            try:
                # perform() returns a future, await it to get the response bytes
                response_bytes_future = await self._ble_client.perform(command)
                raw_response_bytes = await asyncio.wait_for(response_bytes_future, timeout=BluetoothClient.RESPONSE_TIMEOUT + 2)


                if raw_response_bytes:
                    # DeviceCommand.parse_response extracts the data payload from the Modbus frame
                    data_body = command.parse_response(raw_response_bytes)
                    # Specific device instance's parse method decodes the data_body based on register address
                    parsed_fields = self._specific_device_instance.parse(command.starting_address, data_body)
                    all_polled_data.update(parsed_fields)
                    logger.debug(f"Bluetti Wrapper ({self.mac_address}): Parsed for {command.starting_address}: {parsed_fields}")
                else:
                    logger.warning(f"Bluetti Wrapper ({self.mac_address}): No response received for command {command!r}")

            except asyncio.TimeoutError:
                logger.warning(f"Bluetti Wrapper ({self.mac_address}): Timeout waiting for response to command {command!r}")
            except (ModbusError, ParseError) as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Modbus/Parse error for command {command!r}: {e}")
            except (BadConnectionError, BleakError) as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Connection error during command {command!r}: {e}. Client will attempt reconnect.")
                return None # End this poll cycle; rely on client's run loop
            except Exception as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Unexpected error polling command {command!r}: {e}", exc_info=True)
                return None # End this poll cycle

        # Polling for pack_logging_commands would go here if needed, including pack switching logic.
        # For now, focusing on primary polling_commands.

        logger.debug(f"Bluetti Wrapper ({self.mac_address}): Poll cycle completed. Data: {all_polled_data if all_polled_data else 'None'}")
        return all_polled_data if all_polled_data else None

    async def run_polling_loop(self):
        """Main async loop for this device: connects, then polls periodically."""
        self._is_running = True
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Starting polling loop.")

        if not await self._discover_and_init_ble_components():
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Failed to initialize. Stopping.")
            self._is_running = False
            return

        # Start the BluetoothClient's internal run loop as a separate, managed task
        # This task handles connection, disconnections, encryption, and command execution.
        self._client_task = asyncio.create_task(self._ble_client.run(), name=f"BluettiClientRun-{self.mac_address}")
        
        # Short delay to allow client to attempt initial connection
        await asyncio.sleep(2) 

        try:
            while not self._stop_event.is_set():
                if not self._ble_client.is_ready:
                    logger.info(f"Bluetti Wrapper ({self.mac_address}): Client not ready. Waiting...")
                    # Check if client task has exited
                    if self._client_task.done():
                        try:
                            await self._client_task # Await to raise potential exceptions from client task
                        except Exception as e:
                            logger.error(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task exited with error: {e}", exc_info=True)
                        else:
                            logger.error(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task exited unexpectedly. Stopping polling.")
                        break # Exit polling loop
                    await asyncio.sleep(5) # Wait longer if client is not ready
                    continue

                logger.debug(f"Bluetti Wrapper ({self.mac_address}): Initiating poll.")
                polled_data = await self._poll_device_data_once()

                if polled_data:
                    # Send data back to the main application via the callback
                    await self.data_callback({
                        'source': 'bluetti',
                        'device_id': self.mac_address, # Or self._device_ble_name or self._specific_device_instance.sn
                        'model': self._specific_device_instance.type if self._specific_device_instance else 'Unknown',
                        'data': polled_data
                    })
                
                # Wait for the next polling interval or until stop_event is set
                try:
                    logger.debug(f"Bluetti Wrapper ({self.mac_address}): Next poll in {self.polling_interval_seconds}s.")
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.polling_interval_seconds)
                except asyncio.TimeoutError:
                    pass # Normal timeout, continue to next poll
        
        except asyncio.CancelledError:
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Polling loop cancelled.")
        except Exception as e:
            logger.error(f"Bluetti Wrapper ({self.mac_address}): Unhandled exception in polling loop: {e}", exc_info=True)
        finally:
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Polling loop finished.")
            await self._shutdown_client()
            self._is_running = False
            self._is_initialized = False


    async def _shutdown_client(self):
        if self._client_task and not self._client_task.done():
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Cancelling BluetoothClient task...")
            self._client_task.cancel()
            try:
                await self._client_task
            except asyncio.CancelledError:
                logger.info(f"Bluetti Wrapper ({self.mac_address}): BluetoothClient task successfully cancelled.")
            except Exception as e: # Catch other exceptions that might be raised on cancellation
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Error during BluetoothClient task cancellation: {e}")
        
        # BluetoothClient.run() should handle disconnecting its BleakClient in its finally block.
        # If we want to be extra sure or if the client task died:
        if self._ble_client and self._ble_client.client and self._ble_client.client.is_connected:
            try:
                logger.info(f"Bluetti Wrapper ({self.mac_address}): Ensuring BleakClient is disconnected.")
                await self._ble_client.client.disconnect()
            except Exception as e:
                logger.error(f"Bluetti Wrapper ({self.mac_address}): Error during final disconnect: {e}")


    async def start(self):
        """Starts the main polling loop for this device if not already running."""
        if self._is_running:
            logger.warning(f"Bluetti Wrapper ({self.mac_address}): Start called but already running.")
            return
        self._stop_event.clear()
        self._main_task = asyncio.create_task(self.run_polling_loop(), name=f"BluettiPollLoop-{self.mac_address}")
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Main task created.")

    async def stop(self):
        """Signals the polling loop to stop and waits for it to clean up."""
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Stop called.")
        if not self._is_running and not self._main_task:
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Was not running.")
            return

        self._stop_event.set()
        if self._main_task and not self._main_task.done():
            logger.info(f"Bluetti Wrapper ({self.mac_address}): Waiting for main task to complete...")
            try:
                await asyncio.wait_for(self._main_task, timeout=self.polling_interval_seconds + 5) # Give it time to finish current poll + client shutdown
            except asyncio.TimeoutError:
                logger.warning(f"Bluetti Wrapper ({self.mac_address}): Timeout waiting for main task to stop. Attempting cancellation.")
                self._main_task.cancel()
                try:
                    await self._main_task
                except asyncio.CancelledError:
                    logger.info(f"Bluetti Wrapper ({self.mac_address}): Main task cancelled after timeout.")
                except Exception as e:
                    logger.error(f"Bluetti Wrapper ({self.mac_address}): Exception after cancelling main task: {e}")

            except Exception as e:
                 logger.error(f"Bluetti Wrapper ({self.mac_address}): Exception while waiting for main task: {e}")
        logger.info(f"Bluetti Wrapper ({self.mac_address}): Stopped.")
        self._main_task = None
        self._is_running = False