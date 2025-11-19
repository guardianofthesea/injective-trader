import argparse
import asyncio
import sys
from pathlib import Path

from injective_trader import TraderClient

try:
    from SimpleStrategy import SimpleStrategy
except ImportError:
    SimpleStrategy = None

async def main(name: str, config_path: str, log_path: str, debug: bool = False, network: str = "mainnet"):
    """
    Main entry point for the trading bot.
    """
    #######################################################
    ### Initialize Client, Markets, Accounts and Agents ###
    #######################################################

    client = TraderClient(
        config_path=config_path,
        network=network,
        log_path=log_path,
        debug=debug,
    )

    # Add any custom components here [Optional]
    # Example:
    client.add_component("ValkeyListener") # Add this only when you have external VALKEY connections

    await client.initialize()

    ###########################
    ### Add Strategies Here ###
    ###########################
    if SimpleStrategy is None:
        print("SimpleStrategy class not found. Ensure it is defined.")
        sys.exit(1)

    # Add and initialize the custom strategy here [Required]
    # Example:
    await client.add_strategy("SimpleStrategy", SimpleStrategy)

    ######################
    ### Run the Client ###
    ######################
    try:
        client.logger.info(f"Starting {name} trading bot...")
        await client.start()
    except KeyboardInterrupt:
        client.logger.info("Received shutdown signal, stopping the bot...")
    except Exception as e:
        client.logger.error(f"An error occurred: {e}")
        raise
    finally:
        client.logger.info("Shutting down the client...")
        await client.stop()
        print(f"Bot {name} stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Injective Trading Bot")
    parser.add_argument("name", help="Bot name")
    parser.add_argument("config_path", help="Path to config file (YAML)")
    parser.add_argument("log_path", help="Path for log file")
    parser.add_argument("--network", default="mainnet", choices=["mainnet", "testnet"])
    parser.add_argument(
        "--debug",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable asyncio debug mode, default is True.",
    )

    args = parser.parse_args()

    # Validate paths
    if not Path(args.config_path).exists():
        print(f"Error: Config file not found: {args.config_path}")
        sys.exit(1)

    # Create log directory
    Path(args.log_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        asyncio.run(main(
            name=args.name,
            config_path=args.config_path,
            log_path=args.log_path,
            network=args.network,
            debug=args.debug
        ))
    except KeyboardInterrupt:
        print("Bot stopped by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
