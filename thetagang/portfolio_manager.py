"""
Portfolio Manager with multi-broker support (IBKR and Schwab).

This module manages the portfolio strategy across different brokers,
using a common broker interface for abstraction.
"""
import logging
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pathlib import Path

# Broker abstraction imports
from .brokers.base import BaseBroker, Position, Contract, Order
from .brokers.schwab import SchwabBroker

# Keep IBKR imports for backward compatibility
try:
    from ib_async import IB, Stock, Option, util
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False
    IB = None

logger = logging.getLogger(__name__)


class PortfolioManager:
    """
    Manages portfolio operations across different brokers.
    
    This class handles the core strategy logic and delegates
    broker-specific operations to the appropriate broker implementation.
    """

    def __init__(self, config: Dict[str, Any], ib: Optional[IB] = None):
        """
        Initialize the portfolio manager.
        
        Args:
            config: Configuration dictionary from thetagang.toml
            ib: Optional IB connection (for backward compatibility)
        """
        self.config = config
        self.ib = ib  # Keep for backward compatibility with IBKR
        self.broker: Optional[BaseBroker] = None
        self.broker_type = config["account"].get("broker", "ibkr").lower()
        
        # Cache for positions and account data
        self._positions_cache: List[Position] = []
        self._account_info_cache: Dict[str, Any] = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(seconds=30)

    async def initialize(self):
        """Initialize the broker connection."""
        logger.info(f"Initializing portfolio manager with {self.broker_type} broker")
        
        if self.broker_type == "schwab":
            await self._initialize_schwab()
        elif self.broker_type == "ibkr":
            await self._initialize_ibkr()
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")
        
        logger.info(f"Portfolio manager initialized successfully with {self.broker_type}")

    async def _initialize_schwab(self):
        """Initialize Schwab broker."""
        logger.info("Setting up Schwab broker connection")
        
        schwab_config = self.config["account"]["schwab"]
        
        # Create Schwab broker instance
        self.broker = SchwabBroker(
            app_key=schwab_config["app_key"],
            app_secret=schwab_config["app_secret"],
            redirect_uri=schwab_config["redirect_uri"],
            account_number=schwab_config["account_number"],
            token_path=Path(
                schwab_config.get("token_path", "~/.thetagang/schwab_tokens.json")
            ).expanduser(),
        )
        
        # Connect to Schwab
        connected = await self.broker.connect()
        if not connected:
            raise RuntimeError("Failed to connect to Schwab API")
        
        logger.info("Schwab broker connected successfully")

    async def _initialize_ibkr(self):
        """Initialize IBKR broker (backward compatibility)."""
        logger.info("Setting up IBKR broker connection")
        
        if not IBKR_AVAILABLE:
            raise RuntimeError("IBKR support not available. Install ib_async.")
        
        if self.ib is None:
            raise RuntimeError("IB connection not provided for IBKR broker")
        
        # TODO: Wrap existing IBKR functionality in a broker class
        # For now, we'll use the legacy IB object directly
        logger.info("Using legacy IBKR connection")

    async def disconnect(self):
        """Disconnect from the broker."""
        if self.broker:
            await self.broker.disconnect()
            logger.info(f"Disconnected from {self.broker_type}")

    # ===================================================================
    # Account Information Methods
    # ===================================================================

    async def get_account_info(self) -> Dict[str, Any]:
        """
        Get account information (balances, buying power, etc.).
        
        Returns:
            Dictionary with account information
        """
        if self._is_cache_valid():
            return self._account_info_cache
        
        if self.broker_type == "schwab":
            account_info = await self.broker.get_account_info()
        elif self.broker_type == "ibkr":
            account_info = await self._get_ibkr_account_info()
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")
        
        # Update cache
        self._account_info_cache = account_info
        self._cache_time = datetime.now()
        
        return account_info

    async def _get_ibkr_account_info(self) -> Dict[str, Any]:
        """Get account info from IBKR (legacy method)."""
        # Get account summary from IBKR
        account_values = await self.ib.accountSummaryAsync()
        
        # Parse IBKR account values
        account_info = {}
        for av in account_values:
            if av.tag == "BuyingPower":
                account_info["buying_power"] = Decimal(av.value)
            elif av.tag == "TotalCashValue":
                account_info["cash"] = Decimal(av.value)
            elif av.tag == "NetLiquidation":
                account_info["net_liquidation"] = Decimal(av.value)
            elif av.tag == "GrossPositionValue":
                account_info["equity"] = Decimal(av.value)
            elif av.tag == "MaintMarginReq":
                account_info["maintenance_margin"] = Decimal(av.value)
        
        return account_info

    async def get_buying_power(self) -> Decimal:
        """Get available buying power."""
        account_info = await self.get_account_info()
        return account_info.get("buying_power", Decimal(0))

    async def get_net_liquidation(self) -> Decimal:
        """Get net liquidation value."""
        account_info = await self.get_account_info()
        return account_info.get("net_liquidation", Decimal(0))

    # ===================================================================
    # Position Management Methods
    # ===================================================================

    async def get_positions(self, force_refresh: bool = False) -> List[Position]:
        """
        Get all current positions.
        
        Args:
            force_refresh: Force a refresh of the cache
            
        Returns:
            List of Position objects
        """
        if not force_refresh and self._is_cache_valid():
            return self._positions_cache
        
        if self.broker_type == "schwab":
            positions = await self.broker.get_positions()
        elif self.broker_type == "ibkr":
            positions = await self._get_ibkr_positions()
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")
        
        # Update cache
        self._positions_cache = positions
        self._cache_time = datetime.now()
        
        return positions

    async def _get_ibkr_positions(self) -> List[Position]:
        """Get positions from IBKR (legacy method)."""
        portfolio_items = self.ib.portfolio()
        
        positions = []
        for item in portfolio_items:
            position = Position(
                symbol=item.contract.symbol,
                quantity=int(item.position),
                average_cost=Decimal(str(item.averageCost)),
                market_value=Decimal(str(item.marketValue)),
                unrealized_pnl=Decimal(str(item.unrealizedPNL)),
            )
            positions.append(position)
        
        return positions

    async def get_position_by_symbol(self, symbol: str) -> Optional[Position]:
        """Get position for a specific symbol."""
        positions = await self.get_positions()
        for pos in positions:
            if pos.symbol == symbol:
                return pos
        return None

    async def get_stock_positions(self) -> List[Position]:
        """Get only stock positions (excludes options)."""
        positions = await self.get_positions()
        # Filter for stocks only - assumes option symbols have underscores or special chars
        return [pos for pos in positions if "_" not in pos.symbol]

    async def get_option_positions(self) -> List[Position]:
        """Get only option positions."""
        positions = await self.get_positions()
        # Filter for options - assumes option symbols have underscores
        return [pos for pos in positions if "_" in pos.symbol]

    # ===================================================================
    # Market Data Methods
    # ===================================================================

    async def get_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Get market data for specified symbols.
        
        Args:
            symbols: List of symbols to get data for
            
        Returns:
            Dictionary mapping symbols to market data
        """
        if self.broker_type == "schwab":
            return await self.broker.get_market_data(symbols)
        elif self.broker_type == "ibkr":
            return await self._get_ibkr_market_data(symbols)
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")

    async def _get_ibkr_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market data from IBKR (legacy method)."""
        quotes = {}
        
        for symbol in symbols:
            try:
                # Create stock contract
                contract = Stock(symbol, "SMART", currency="USD")
                
                # Request market data
                ticker = self.ib.reqMktData(contract, "", False, False)
                
                # Wait for data
                await self.ib.sleepAsync(2)
                
                quotes[symbol] = {
                    "last": Decimal(str(ticker.last)) if ticker.last else None,
                    "bid": Decimal(str(ticker.bid)) if ticker.bid else None,
                    "ask": Decimal(str(ticker.ask)) if ticker.ask else None,
                    "volume": int(ticker.volume) if ticker.volume else 0,
                    "close": Decimal(str(ticker.close)) if ticker.close else None,
                }
                
                # Cancel market data
                self.ib.cancelMktData(contract)
                
            except Exception as e:
                logger.error(f"Failed to get market data for {symbol}: {e}")
                quotes[symbol] = None
        
        return quotes

    async def get_stock_price(self, symbol: str) -> Optional[Decimal]:
        """Get current stock price for a symbol."""
        quotes = await self.get_market_data([symbol])
        quote = quotes.get(symbol)
        if quote:
            return quote.get("last") or quote.get("bid")
        return None

    # ===================================================================
    # Order Management Methods
    # ===================================================================

    async def place_option_order(
        self,
        symbol: str,
        strike: Decimal,
        expiration: datetime,
        right: str,
        quantity: int,
        action: str,
        order_type: str = "LIMIT",
        limit_price: Optional[Decimal] = None,
    ) -> str:
        """
        Place an option order.
        
        Args:
            symbol: Underlying symbol
            strike: Strike price
            expiration: Expiration date
            right: 'CALL' or 'PUT'
            quantity: Number of contracts
            action: 'BUY' or 'SELL'
            order_type: 'MARKET' or 'LIMIT'
            limit_price: Limit price (required for LIMIT orders)
            
        Returns:
            Order ID
        """
        # Create contract and order objects
        contract = Contract(
            symbol=symbol,
            strike=strike,
            expiration=expiration,
            right=right.upper(),
            multiplier=100,
        )
        
        order = Order(
            contract=contract,
            action=action.upper(),
            quantity=quantity,
            order_type=order_type.upper(),
            limit_price=limit_price,
        )
        
        if self.broker_type == "schwab":
            order_id = await self.broker.place_order(order)
        elif self.broker_type == "ibkr":
            order_id = await self._place_ibkr_option_order(order)
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")
        
        logger.info(
            f"Placed {action} order for {quantity} {symbol} {strike} {right} "
            f"exp {expiration.strftime('%Y-%m-%d')}, order_id: {order_id}"
        )
        
        return order_id

    async def _place_ibkr_option_order(self, order: Order) -> str:
        """Place option order with IBKR (legacy method)."""
        from ib_async import Order as IBOrder
        
        # Create IBKR option contract
        ib_contract = Option(
            symbol=order.contract.symbol,
            lastTradeDateOrContractMonth=order.contract.expiration.strftime("%Y%m%d"),
            strike=float(order.contract.strike),
            right=order.contract.right[0],  # 'C' or 'P'
            exchange="SMART",
            currency="USD",
        )
        
        # Create IBKR order
        ib_order = IBOrder()
        ib_order.action = order.action
        ib_order.totalQuantity = order.quantity
        ib_order.orderType = order.order_type
        
        if order.order_type == "LIMIT" and order.limit_price:
            ib_order.lmtPrice = float(order.limit_price)
        
        # Place order
        trade = self.ib.placeOrder(ib_contract, ib_order)
        
        # Wait for order to be submitted
        await self.ib.sleepAsync(1)
        
        return str(trade.order.orderId)

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel an order.
        
        Args:
            order_id: ID of the order to cancel
            
        Returns:
            True if successful, False otherwise
        """
        if self.broker_type == "schwab":
            success = await self.broker.cancel_order(order_id)
        elif self.broker_type == "ibkr":
            success = await self._cancel_ibkr_order(order_id)
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")
        
        if success:
            logger.info(f"Cancelled order {order_id}")
        else:
            logger.error(f"Failed to cancel order {order_id}")
        
        return success

    async def _cancel_ibkr_order(self, order_id: str) -> bool:
        """Cancel order with IBKR (legacy method)."""
        try:
            # Find the order
            trades = self.ib.trades()
            for trade in trades:
                if str(trade.order.orderId) == order_id:
                    self.ib.cancelOrder(trade.order)
                    return True
            
            logger.warning(f"Order {order_id} not found")
            return False
            
        except Exception as e:
            logger.error(f"Error cancelling IBKR order {order_id}: {e}")
            return False

    async def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        if self.broker_type == "schwab":
            return await self.broker.get_open_orders()
        elif self.broker_type == "ibkr":
            return await self._get_ibkr_open_orders()
        else:
            raise ValueError(f"Unsupported broker: {self.broker_type}")

    async def _get_ibkr_open_orders(self) -> List[Order]:
        """Get open orders from IBKR (legacy method)."""
        # TODO: Convert IBKR trades to Order objects
        orders = []
        trades = self.ib.openTrades()
        
        for trade in trades:
            # Parse IBKR order into our Order format
            # This is simplified - full implementation would parse all fields
            pass
        
        return orders

    # ===================================================================
    # Strategy Methods (Broker-agnostic)
    # ===================================================================

    async def calculate_target_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Calculate target positions based on config.
        
        Returns:
            Dictionary mapping symbols to target allocations
        """
        symbols_config = self.config.get("symbols", {})
        net_liq = await self.get_net_liquidation()
        
        targets = {}
        
        for symbol, symbol_config in symbols_config.items():
            weight = symbol_config.get("weight", 0)
            target_value = net_liq * Decimal(str(weight))
            
            targets[symbol] = {
                "weight": weight,
                "target_value": target_value,
                "delta": symbol_config.get("delta", 0.30),
                "dte": symbol_config.get("dte") or self.config["target"].get("dte", 45),
            }
        
        return targets

    async def check_and_write_puts(self):
        """
        Check if we should write new puts and do so if conditions are met.
        
        This is the core strategy logic for writing puts.
        """
        logger.info("Checking if we should write puts")
        
        # Get current state
        account_info = await self.get_account_info()
        positions = await self.get_positions()
        targets = await self.calculate_target_positions()
        
        buying_power = account_info["buying_power"]
        
        for symbol, target_info in targets.items():
            # Check if we should write puts for this symbol
            should_write = await self._should_write_puts(
                symbol, target_info, positions, buying_power
            )
            
            if should_write:
                await self._write_puts_for_symbol(symbol, target_info, buying_power)

    async def _should_write_puts(
        self,
        symbol: str,
        target_info: Dict[str, Any],
        positions: List[Position],
        buying_power: Decimal,
    ) -> bool:
        """
        Determine if we should write puts for a symbol.
        
        Args:
            symbol: Stock symbol
            target_info: Target allocation info
            positions: Current positions
            buying_power: Available buying power
            
        Returns:
            True if we should write puts
        """
        # Get write_when config
        write_when = self.config.get("write_when", {})
        puts_config = write_when.get("puts", {})
        
        # Check if puts are enabled
        if not puts_config.get("green", True):
            logger.debug(f"Put writing disabled for green days for {symbol}")
            return False
        
        # Calculate current allocation
        current_position = await self.get_position_by_symbol(symbol)
        current_value = (
            current_position.market_value if current_position else Decimal(0)
        )
        
        target_value = target_info["target_value"]
        
        # Check if we're under-allocated
        if current_value >= target_value:
            logger.debug(f"{symbol}: At or above target allocation")
            return False
        
        # Check if we have enough buying power
        stock_price = await self.get_stock_price(symbol)
        if not stock_price:
            logger.warning(f"Could not get price for {symbol}")
            return False
        
        # Calculate buying power needed for one contract
        contracts_needed = 1
        bp_needed = stock_price * Decimal(100) * Decimal(contracts_needed)
        
        if bp_needed > buying_power:
            logger.debug(f"{symbol}: Insufficient buying power")
            return False
        
        return True

    async def _write_puts_for_symbol(
        self, symbol: str, target_info: Dict[str, Any], buying_power: Decimal
    ):
        """
        Write puts for a specific symbol.
        
        Args:
            symbol: Stock symbol
            target_info: Target allocation info
            buying_power: Available buying power
        """
        logger.info(f"Writing puts for {symbol}")
        
        # Get current stock price
        stock_price = await self.get_stock_price(symbol)
        if not stock_price:
            logger.error(f"Could not get price for {symbol}")
            return
        
        # Calculate strike based on delta
        delta = target_info["delta"]
        target_dte = target_info["dte"]
        
        # Calculate strike price (simplified)
        # In reality, you'd use option chain data and calculate delta
        strike = stock_price * Decimal(str(1 - delta))
        strike = strike.quantize(Decimal("0.5"))  # Round to nearest 0.5
        
        # Calculate expiration date
        expiration = datetime.now() + timedelta(days=target_dte)
        
        # Place the order
        try:
            order_id = await self.place_option_order(
                symbol=symbol,
                strike=strike,
                expiration=expiration,
                right="PUT",
                quantity=1,
                action="SELL",
                order_type="LIMIT",
                limit_price=stock_price * Decimal("0.01"),  # Simplified pricing
            )
            
            logger.info(f"Successfully wrote put for {symbol}, order_id: {order_id}")
            
        except Exception as e:
            logger.error(f"Failed to write put for {symbol}: {e}", exc_info=True)

    async def check_and_roll_positions(self):
        """
        Check if we should roll any positions and do so if conditions are met.
        
        This checks existing option positions and rolls them if needed.
        """
        logger.info("Checking if we should roll positions")
        
        option_positions = await self.get_option_positions()
        
        for position in option_positions:
            should_roll = await self._should_roll_position(position)
            
            if should_roll:
                await self._roll_position(position)

    async def _should_roll_position(self, position: Position) -> bool:
        """
        Determine if we should roll a position.
        
        Args:
            position: The position to check
            
        Returns:
            True if we should roll
        """
        roll_when = self.config.get("roll_when", {})
        
        # Check P&L threshold
        pnl_threshold = roll_when.get("pnl", 0.9)
        
        # Simplified check - in reality you'd parse the position symbol
        # to get contract details and check DTE, ITM status, etc.
        
        return False  # Placeholder

    async def _roll_position(self, position: Position):
        """
        Roll a position to the next expiration.
        
        Args:
            position: The position to roll
        """
        logger.info(f"Rolling position for {position.symbol}")
        
        # TODO: Implement position rolling logic
        # This would:
        # 1. Parse the current position to get strike, expiration, etc.
        # 2. Find a new contract to roll to
        # 3. Close the current position
        # 4. Open the new position

    # ===================================================================
    # Main Strategy Execution
    # ===================================================================

    async def manage(self):
        """
        Main strategy execution method.
        
        This is the entry point that runs the entire strategy:
        1. Check and write new puts
        2. Check and roll existing positions
        3. Check and write calls (if holding stock)
        """
        logger.info("=" * 80)
        logger.info("Starting portfolio management cycle")
        logger.info("=" * 80)
        
        try:
            # Display current state
            await self._display_account_summary()
            
            # Execute strategy
            await self.check_and_write_puts()
            await self.check_and_roll_positions()
            
            logger.info("Portfolio management cycle complete")
            
        except Exception as e:
            logger.error(f"Error during portfolio management: {e}", exc_info=True)
            raise

    async def _display_account_summary(self):
        """Display current account summary."""
        account_info = await self.get_account_info()
        positions = await self.get_positions()
        
        logger.info("-" * 80)
        logger.info("Account Summary")
        logger.info("-" * 80)
        logger.info(f"Broker: {self.broker_type.upper()}")
        logger.info(f"Net Liquidation: ${account_info['net_liquidation']:,.2f}")
        logger.info(f"Buying Power: ${account_info['buying_power']:,.2f}")
        logger.info(f"Cash: ${account_info['cash']:,.2f}")
        logger.info(f"Total Positions: {len(positions)}")
        logger.info("-" * 80)

    # ===================================================================
    # Utility Methods
    # ===================================================================

    def _is_cache_valid(self) -> bool:
        """Check if the cache is still valid."""
        if self._cache_time is None:
            return False
        return datetime.now() - self._cache_time < self._cache_ttl

    def _invalidate_cache(self):
        """Invalidate the cache."""
        self._cache_time = None
        self._positions_cache = []
        self._account_info_cache = {}
