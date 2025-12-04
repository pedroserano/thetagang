"""
Main ThetaGang entry point with multi-broker support.

This module handles the initialization and connection management
for both IBKR and Schwab brokers.
"""
import asyncio
import logging
import sys
from typing import Optional

from ib_async import IB, util
import toml

from .portfolio_manager import PortfolioManager

logger = logging.getLogger(__name__)


async def start_schwab(config: dict, dry_run: bool = False):
    """
    Start ThetaGang with Schwab broker.
    
    Args:
        config: Configuration dictionary
        dry_run: If True, don't place actual orders
    """
    logger.info("Starting ThetaGang with Schwab broker")
    
    # Create portfolio manager
    portfolio_manager = PortfolioManager(config)
    
    try:
        # Initialize Schwab connection
        await portfolio_manager.initialize()
        
        # Display account info
        logger.info("Successfully connected to Schwab")
        
        # Run the strategy
        if dry_run:
            logger.info("DRY RUN MODE - No orders will be placed")
        
        await portfolio_manager.manage()
        
        logger.info("Strategy execution complete")
        
    except Exception as e:
        logger.error(f"Error during Schwab execution: {e}", exc_info=True)
        raise
    
    finally:
        # Cleanup
        await portfolio_manager.disconnect()


async def start_ibkr(config: dict, dry_run: bool = False, without_ibc: bool = False):
    """
    Start ThetaGang with IBKR broker (legacy mode).
    
    Args:
        config: Configuration dictionary
        dry_run: If True, don't place actual orders
        without_ibc: If True, don't start IBC
    """
    logger.info("Starting ThetaGang with IBKR broker")
    
    # IB connection settings
    ib_config = config.get("ib_async", {})
    
    # Create IB connection
    ib = IB()
    
    try:
        # Connect to IBKR
        if without_ibc:
            # Connect to existing gateway
            logger.info("Connecting to existing IBKR gateway")
            host = config.get("host", "127.0.0.1")
            port = config.get("port", 4002)
            await ib.connectAsync(host=host, port=port, clientId=1)
        else:
            # Use IBC to start gateway
            logger.info("Starting IBKR gateway with IBC")
            from ib_async import Watchdog
            
            watchdog = Watchdog(ib, config)
            watchdog.start()
            await watchdog.connectAsync()
        
        logger.info("Successfully connected to IBKR")
        
        # Create portfolio manager with IB connection
        portfolio_manager = PortfolioManager(config, ib=ib)
        await portfolio_manager.initialize()
        
        # Run the strategy
        if dry_run:
            logger.info("DRY RUN MODE - No orders will be placed")
        
        await portfolio_manager.manage()
        
        logger.info("Strategy execution complete")
        
    except Exception as e:
        logger.error(f"Error during IBKR execution: {e}", exc_info=True)
        raise
    
    finally:
        # Cleanup
        if ib.isConnected():
            ib.disconnect()


def start(config_path: str, without_ibc: bool = False, dry_run: bool = False):
    """
    Main entry point for ThetaGang.
    
    This function:
    1. Loads the configuration
    2. Determines which broker to use
    3. Starts the appropriate broker connection
    4. Runs the portfolio management strategy
    
    Args:
        config_path: Path to the thetagang.toml config file
        without_ibc: Don't start IBC (for IBKR only)
        dry_run: Don't place actual orders
    """
    # Load configuration
    try:
        with open(config_path, "r") as f:
            config = toml.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)
    
    # Determine broker type
    broker_type = config.get("account", {}).get("broker", "ibkr").lower()
    
    logger.info(f"ThetaGang starting with broker: {broker_type}")
    
    # Start with the appropriate broker
    if broker_type == "schwab":
        asyncio.run(start_schwab(config, dry_run=dry_run))
    elif broker_type == "ibkr":
        asyncio.run(start_ibkr(config, dry_run=dry_run, without_ibc=without_ibc))
    else:
        logger.error(f"Unsupported broker: {broker_type}")
        sys.exit(1)


def setup_logging(config: dict):
    """
    Set up logging configuration.
    
    Args:
        config: Configuration dictionary
    """
    log_level = config.get("logging", {}).get("level", "INFO")
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    
    # Set specific log levels for noisy libraries
    logging.getLogger("ib_async").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


if __name__ == "__main__":
    # This allows running the module directly for testing
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m thetagang.thetagang <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    start(config_path)
