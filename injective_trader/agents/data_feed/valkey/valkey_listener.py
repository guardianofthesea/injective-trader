import json
import asyncio
from typing import Dict, Optional, Any, cast
from decimal import Decimal

import valkey.asyncio as valkey
from valkey.exceptions import ValkeyError, ConnectionError as ValkeyConnectionError, TimeoutError as ValkeyTimeoutError
from injective_trader.core.component import Component
from injective_trader.utils.enums import Event
from injective_trader.utils.retry import get_retry_handler
from injective_trader.domain.message import Notification


class ValkeyListener(Component):
    """
    Listens to a Valkey Pub/Sub channel and processes messages.
    Handles connection, reconnection, and message processing.
    """

    def __init__(self, logger, composer_base):
        super().__init__(logger, composer_base)
        self.name = "ValkeyListener"

        # Configuration placeholders - will be set in initialize
        self.config: dict[str, Any] = {}
        self.valkey_channels: list[str] = []
        self.valkey_url: None|str = None
        self.retry_handler = None # For specific operations, not the main loop
        self.reconnection_delay: int = 5
        self._notification_type: str = "external" # Default, can be configured

        # Task management
        self._reader_task: None|asyncio.Task = None
        self._is_running: bool = False # Flag to control the main run loop
        self._timeout = 0.5

    async def initialize(self, **kwargs: Any):
        """
        Initialize ValkeyListener with configuration.
        """
        self.config = kwargs.get("config", {})
        self.valkey_channels = self.config.get('ValkeyChannels', [])
        self.valkey_url = self.config.get('ValkeyUrl')

        if not self.valkey_channels or not self.valkey_url:
            self.logger.error("ValkeyChannels and ValkeyUrl must be provided in configuration.")
            raise ValueError("Missing ValkeyChannels or ValkeyUrl in configuration")

        retry_config = self.config.get("RetryConfig", {})
        self.retry_handler = get_retry_handler(
            self.logger,
            retry_config,
            f"{self.name}Operation" # More specific name for retry handler context
        )
        self.reconnection_delay = self.config.get("ReconnectionDelay", 5)
        self._notification_type = self.config.get("NotificationType", "external") # e.g. "stork" or "external"

        self.logger.info(f"{self.name} initialized for channels '{', '.join(self.valkey_channels)}' at '{self.valkey_url}'")

    async def run(self, **kwargs: Any):
        """
        Main component run loop. Manages connection and message reading.
        """
        self._is_running = True
        self.logger.info(f"Starting {self.name} run loop.")

        while self._is_running:
            try:
                await self._init_valkey_connection()
                if self._reader_task is None or self._reader_task.done():
                    self.logger.info("Creating Valkey reader task.")
                    self._reader_task = asyncio.create_task(self._valkey_reader(), name="ValkeyReaderTask")
                await self._reader_task

            except (ValkeyConnectionError, ValkeyTimeoutError, ValkeyError) as e:
                self.logger.error(f"Valkey connection/protocol error: {e}. Attempting to reconnect in {self.reconnection_delay}s.")
                await self._cleanup_transient_resources()
                if self._is_running: # Only sleep if we are supposed to be running
                    await asyncio.sleep(self.reconnection_delay)
            except asyncio.CancelledError:
                self.logger.info(f"{self.name} run loop was cancelled.")
                self._is_running = False # Ensure loop terminates
                break
            except Exception as e:
                self.logger.error(f"Unexpected error in {self.name} run loop: {e}", exc_info=True)
                self._is_running = False # Stop on unhandled errors to prevent loops
                break # For safety, stop on unknown errors
            finally:
                # ensure the reader task is None so it gets recreated.
                if self._is_running and self._reader_task and self._reader_task.done():
                     # Log the reason if task finished with exception
                    if self._reader_task.exception():
                        self.logger.warning(f"Reader task finished with exception: {self._reader_task.exception()}")
                    self._reader_task = None

        self.logger.info(f"{self.name} run loop finished.")
        await self._cleanup_all_resources()

    async def _init_valkey_connection(self):
        """
        Initialize Valkey client connection and subscribe to the channel.
        Raises Valkey errors on failure.
        """
        if not self.valkey_url or not self.valkey_channels:
            raise ValueError("Valkey URL or Channels not configured.")

        self.logger.info(f"Attempting to connect to Valkey at {self.valkey_url}...")
        try:
            await self._cleanup_transient_resources()
        except Exception as e:
            pass

        self._client = await valkey.from_url(self.valkey_url)
        self.logger.info("Successfully connected to Valkey.")

        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(*self.valkey_channels)
        self.logger.info(f"Successfully subscribed to Valkey channels: {', '.join(self.valkey_channels)}")

    async def _valkey_reader(self):
        """
        Reads messages from the Valkey pubsub channels and processes them.
        """
        if not self._pubsub:
            self.logger.error("Valkey pubsub not initialized. Reader cannot start.")
            raise ValkeyError("PubSub not initialized for reader.")

        self.logger.info(f"Valkey message reader started for channels: {', '.join(self.valkey_channels)}.")
        try:
            while self._is_running:
                try:
                    message = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=self._timeout)
                except (ValkeyConnectionError, ValkeyTimeoutError) as e:
                    self.logger.error(f"Connection error while getting message: {e}")
                    raise
                except ValkeyError as e:
                    self.logger.error(f"Valkey client error in reader: {e}")
                    raise

                if not message:
                    await asyncio.sleep(0.01)
                    continue

                self.logger.debug(f"Received Valkey message: {message}")

                # The channel field tells us which channel this message came from
                # It will be bytes, so decode it
                channel_bytes = message.get("channel")
                origin_channel: str = ''
                if isinstance(channel_bytes, bytes):
                    origin_channel = channel_bytes.decode('utf-8')

                if message.get("type") == "message" and message.get("data") == b"STOP":
                    # Note: A STOP message on one channel might not mean stop all.
                    # You might want a specific control channel for STOP,
                    # or a mechanism to stop listening to a specific channel.
                    # For now, any STOP on any subscribed channel stops the listener.
                    self.logger.info(f"Received STOP message on channel '{origin_channel}'. Stopping Valkey reader.")
                    self._is_running = False
                    break

                if message.get("type") == "message" and origin_channel:
                    await self._process_message(message=message, origin_channel=origin_channel)

        except asyncio.CancelledError:
            self.logger.info("Valkey reader task was cancelled.")
            raise
        except Exception as e:
            self.logger.error(f"Error in Valkey reader: {e}", exc_info=True)
            raise
        finally:
            self.logger.info("Valkey reader task finished.")

    async def _process_message(self, *, message: dict[str, Any], origin_channel: str):
        """
        Process a single message received from Valkey.
        'origin_channel' is the name of the channel this message came from.
        """
        try:
            message_data = message.get("data")
            if not isinstance(message_data, bytes):
                self.logger.warning(f"Received message on channel '{origin_channel}' with non-bytes data: {type(message_data)}")
                return

            raw_payload = json.loads(message_data.decode('utf-8'))
            self.logger.debug(f"Channel '{origin_channel}' raw payload: {raw_payload}")
            if not isinstance(raw_payload, dict):
                self.logger.warning(f"Received non-dict JSON data on channel '{origin_channel}': {raw_payload}")
                return

            await self._notify_update(raw_payload=raw_payload, channel_name=origin_channel)

        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from Valkey message on channel '{origin_channel}': {message.get('data')}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Error processing Valkey message from channel '{origin_channel}': {e}", exc_info=True)

    async def _notify_update(self, *, raw_payload: dict, channel_name: str):
        """
        Send an EXTERNAL_INFO notification with the processed data.
        'channel_name' is used as the symbol.
        """
        try:
            data_to_send = {
                "channel": channel_name, # Use the actual channel name
                "type": self._notification_type,
                # FIXME: if market id is not included, the strategy side won't be able to receive update.
                **raw_payload,
            }
            self.logger.debug(f"data to send: {data_to_send}")

            notification = Notification(
                event=Event.EXTERNAL_INFO,
                data=data_to_send
            )
            if hasattr(self, 'mediator') and self.mediator:
                await self.mediator.notify(notification)
            else:
                self.logger.warning("Mediator not available, cannot send notification.")
        except Exception as e:
            self.logger.error(f"Error notifying update for channel '{channel_name}': {e}", exc_info=True)


    async def terminate(self, msg: str = "Terminating", **kwargs: Any):
        """
        Cleanly shut down the Valkey listener.
        """
        self.logger.info(f"Terminating {self.name}: {msg}")
        self._is_running = False

        if self._reader_task and not self._reader_task.done():
            self.logger.info("Cancelling Valkey reader task...")
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                self.logger.info("Valkey reader task successfully cancelled.")
            except Exception as e:
                self.logger.error(f"Error while awaiting cancelled reader task: {e}", exc_info=True)

        self.logger.info(f"{self.name} terminated.")
        # _cleanup_all_resources will be called by the run loop's finally block if it exits cleanly,
        # or explicitly if terminate is called before/after run completes.
        # To be safe, one could call it here too if there's a chance run didn't finish its finally.
        # However, the current run loop structure should handle it.

    async def _cleanup_transient_resources(self):
        """Clean up resources that are recreated on reconnection (pubsub, client)."""
        if self._pubsub:
            self.logger.debug("Cleaning up Valkey pubsub...")
            try:
                # Unsubscribe from all channels we might have subscribed to
                if self.valkey_channels and self._pubsub.connection:
                    # Check if pubsub still has channels (it might be disconnected)
                    # pubsub.channels is a dict mapping channel_name_bytes -> handler
                    # We can attempt to unsubscribe from specific channels or all
                    # For simplicity, trying to unsubscribe from all known channels:
                    active_subscribed_channels = [ch for ch in self.valkey_channels if ch.encode() in self._pubsub.channels]
                    if active_subscribed_channels:
                         await self._pubsub.unsubscribe(*active_subscribed_channels)
                    # Alternatively, just `await self._pubsub.unsubscribe()` might work if it clears all.
                    # According to redis-py docs, `unsubscribe()` with no args unsubscribes from all.
                    # If `valkey-py` follows, this is simpler:
                    # await self._pubsub.unsubscribe()

                await self._pubsub.close() # This should also handle unsubscription implicitly
            except ValkeyError as e: # Catch specific Valkey errors if connection is already lost
                self.logger.warning(f"Valkey error during pubsub cleanup (channel unsubscribe/close): {e}")
            except Exception as e:
                self.logger.warning(f"Error during pubsub cleanup: {e}", exc_info=True)
            finally:
                self._pubsub = None

        if self._client:
            self.logger.debug("Cleaning up Valkey client...")
            try:
                await self._client.aclose() # redis-py >= 4.2, valkey-py likely follows
                                            # or await self._client.close() if older pattern
            except Exception as e:
                self.logger.warning(f"Error during client connection cleanup: {e}", exc_info=True)
            finally:
                self._client = None

    async def _cleanup_all_resources(self):
        """Clean up all Valkey connections and tasks."""
        self.logger.info(f"Performing full cleanup for {self.name}...")
        await self._cleanup_transient_resources()

        if self._reader_task and not self._reader_task.done():
            self.logger.warning("Reader task still active during final cleanup. Attempting to cancel.")
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.logger.error(f"Error awaiting lingering reader task during final cleanup: {e}")
        self._reader_task = None
        self.logger.info(f"{self.name} full cleanup complete.")


    async def receive(self, event: Event, data: dict):
        """
        Process events from the mediator (if your component needs to).
        This is a placeholder as per original code.
        """
        self.logger.debug(f"Received event {event} with data: {data}")
        # Implement if this component needs to react to mediator events
        pass
