import argparse
import json
import time
from typing import Dict, Optional, Callable
import signal
import sys

import valkey
import valkey


class ValkeyPriceListener:
    """
    Listens for price updates on a specified Valkey channel.
    Can process messages with a callback function or execute default handling.
    """
    def __init__(
        self,
        channel: str,
        callback: Optional[Callable[[Dict], None]] = None,
        valkey_host: str = "localhost",
        valkey_port: int = 6379,
        valkey_db: int = 0,
        use_valkey: bool = True
    ):
        """
        Initialize the price listener.

        Args:
            channel: The Valkey channel to subscribe to
            callback: Optional function to process received messages
            valkey_host: Valkey server hostname
            valkey_port: Valkey server port
            valkey_db: Valkey database number
            use_valkey: Whether to use Valkey client instead of Valkey client
        """
        self.channel = channel
        self.callback = callback
        self.running = False

        # Create the appropriate client (Valkey or Valkey)
        if use_valkey:
            self.client = valkey.Valkey(
                host=valkey_host,
                port=valkey_port,
                db=valkey_db
            )
        else:
            self.client = valkey.Valkey(
                host=valkey_host,
                port=valkey_port,
                db=valkey_db
            )

        # Create the pubsub object
        self.pubsub = self.client.pubsub()

    def start(self):
        """Start listening for price updates"""
        self.running = True

        # Subscribe to the channel
        self.pubsub.subscribe(self.channel)
        print(f"Subscribed to channel: {self.channel}")

        # Process messages as they come in
        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break

                if message['type'] == 'message':
                    self._process_message(message)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Stop listening for price updates"""
        self.running = False
        self.pubsub.unsubscribe(self.channel)
        print(f"\nUnsubscribed from channel: {self.channel}")

    def _process_message(self, message):
        """Process a received message"""
        try:
            # Extract and parse the data
            data = message.get('data')
            if not data:
                return

            if isinstance(data, bytes):
                data = data.decode('utf-8')

            price_data = json.loads(data)

            # If a callback was provided, use it
            if self.callback:
                self.callback(price_data)
            else:
                # Default processing
                timestamp = price_data.get('timestamp')
                price = price_data.get('price')

                if timestamp and price:
                    human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1e9))
                    print(f"[{human_time}] Price: {price}")
                else:
                    print(f"Received data: {price_data}")
        except json.JSONDecodeError:
            print(f"Error: Could not parse message as JSON: {message}")
        except Exception as e:
            print(f"Error processing message: {e}")


class SignalHandler:
    """Handles system signals for graceful shutdown"""
    def __init__(self, listener: ValkeyPriceListener):
        self.listener = listener
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print("Received shutdown signal, closing connections...")
        self.listener.stop()
        sys.exit(0)



def main():
    """Main function to run the price listener"""
    parser = argparse.ArgumentParser(description="Listen for asset price updates from Valkey")
    parser.add_argument("-a", "--asset", help="Asset to listen for (forms the channel name)")
    parser.add_argument("-c", "--channel", help="Valkey channel name to subscribe to")
    parser.add_argument("--host", default="localhost", help="Valkey server hostname")
    parser.add_argument("--port", type=int, default=6379, help="Valkey server port")
    parser.add_argument("--db", type=int, default=0, help="Valkey database number")
    parser.add_argument("--valkey", action="store_true", help="Use Valkey client instead of Valkey")

    args = parser.parse_args()

    # Determine the channel name
    channel = args.channel
    if not channel and args.asset:
        channel = f"{args.asset.lower()}_price_updates"
    if not channel:
        parser.error("Either --asset or --channel must be specified")

    print(f"Starting price listener for channel: {channel}")
    print(f"Connecting to {'Valkey' if args.valkey else 'Valkey'} at {args.host}:{args.port} (DB: {args.db})")

    # Create and start the listener
    listener = ValkeyPriceListener(
        channel=channel,
        valkey_host=args.host,
        valkey_port=args.port,
        valkey_db=args.db,
        use_valkey=not args.valkey,
        callback=
price_handler
    )

    # Setup signal handler for graceful shutdown
    signal_handler = SignalHandler(listener)

    # Start listening
    try:
        listener.start()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def price_handler(price_data: Dict):
    """Example callback function to process price updates"""
    timestamp = price_data.get('timestamp')
    price = price_data.get('price')

    # Handle oversized timestamps (nanoseconds or microseconds)
    if isinstance(timestamp, (int, float)) and timestamp > 10**12:
        # Convert nanoseconds to seconds if timestamp is too large
        if timestamp > 10**18:  # Likely nanoseconds
            timestamp = timestamp / 10**9
        # Convert microseconds to seconds if timestamp is large but not enormous
        elif timestamp > 10**15:  # Likely microseconds
            timestamp = timestamp / 10**6
        # Convert milliseconds to seconds
        elif timestamp > 10**12:  # Likely milliseconds
            timestamp = timestamp / 10**3

    try:
        human_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        print(f"Price Update at {human_time}: ${price:.9f}")
    except (ValueError, OverflowError):
        # If conversion fails, show the original timestamp
        print(f"Price Update [Raw timestamp: {timestamp}]: ${price:.9f}")

if __name__ == "__main__":
    main()

# Example usage with custom callback:
#
# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         main()
#     else:
#         # Example of using the listener programmatically
#         print("Running with custom handler...")
#         listener = PriceListener("btc_price_updates", callback=custom_price_handler)
#         SignalHandler(listener)
#         listener.start()
