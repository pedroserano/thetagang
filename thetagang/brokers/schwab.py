"""Schwab broker implementation for ThetaGang."""
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from pathlib import Path

from schwab import AsyncSchwabClient

# Note: Import statements below assume schwab-trader library structure
# Adjust based on actual library API
try:
    from schwab.models import OrderInstruction, OrderType, Duration, Session
except ImportError:
    # Fallback for different library versions
    OrderInstruction = None
    OrderType = None
    Duration = None
    Session = None

from .base import BaseBroker, Position, Contract, Order as BaseOrder

logger = logging.getLogger(__name__)


class SchwabBroker(BaseBroker):
    """Schwab broker implementation using schwab-trader library."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        redirect_uri: str,
        account_number: str,
        token_path: Optional[Path] = None,
    ):
        """
        Initialize Schwab broker.

        Args:
            app_key: Schwab API app key
            app_secret: Schwab API app secret
            redirect_uri: OAuth redirect URI
            account_number: Encrypted account number (hash value)
            token_path: Path to store OAuth tokens
        """
        self.app_key = app_key
        self.app_secret = app_secret
        self.redirect_uri = redirect_uri
        self.account_number = account_number
        self.token_path = (
            token_path or Path.home() / ".thetagang" / "schwab_tokens.json"
        )
        self.client: Optional[AsyncSchwabClient] = None

        # Create token directory if needed
        self.token_path.parent.mkdir(parents=True, exist_ok=True)

    async def connect(self) -> bool:
        """Connect to Schwab API."""
        try:
            # Initialize the async client
            self.client = AsyncSchwabClient(
                api_key=self.app_key,
                api_secret=self.app_secret,
                redirect_uri=self.redirect_uri,
                token_path=str(self.token_path),
            )

            # Verify connection by fetching account numbers
            accounts = await self.client.get_account_numbers()
            logger.info(f"Connected to Schwab. Found {len(accounts)} account(s)")

            # Verify our account exists
            account_found = any(
                acc.hashValue == self.account_number for acc in accounts
            )
            if not account_found:
                logger.warning(
                    f"Account {self.account_number} not found in available accounts"
                )
                return False

            return True

        except Exception as e:
            logger.error(f"Failed to connect to Schwab: {e}", exc_info=True)
            return False

    async def disconnect(self) -> None:
        """Disconnect from Schwab API."""
        if self.client:
            await self.client.close()
            self.client = None
            logger.info("Disconnected from Schwab")

    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        account = await self.client.get_account(
            account_number=self.account_number, include_positions=True
        )

        # Extract balance information
        balances = account.securitiesAccount.currentBalances

        return {
            "buying_power": Decimal(str(balances.buyingPower)),
            "cash": Decimal(str(balances.cashBalance)),
            "equity": Decimal(str(balances.equity)),
            "net_liquidation": Decimal(str(balances.liquidationValue)),
            "maintenance_margin": Decimal(
                str(getattr(balances, "maintenanceRequirement", 0))
            ),
        }

    async def get_positions(self) -> List[Position]:
        """Get all current positions."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        account = await self.client.get_account(
            account_number=self.account_number, include_positions=True
        )

        positions = []
        if hasattr(account.securitiesAccount, "positions") and account.securitiesAccount.positions:
            for pos in account.securitiesAccount.positions:
                long_qty = int(pos.longQuantity or 0)
                short_qty = int(pos.shortQuantity or 0)
                net_qty = long_qty - short_qty

                position = Position(
                    symbol=pos.instrument.symbol,
                    quantity=net_qty,
                    average_cost=Decimal(str(pos.averagePrice or 0)),
                    market_value=Decimal(str(pos.marketValue or 0)),
                    unrealized_pnl=Decimal(
                        str(getattr(pos, "currentDayProfitLoss", 0) or 0)
                    ),
                )
                positions.append(position)

        logger.debug(f"Retrieved {len(positions)} positions")
        return positions

    async def get_open_orders(self) -> List[BaseOrder]:
        """Get all open orders."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        # Get orders from last 60 days with WORKING status
        from_date = datetime.now() - timedelta(days=60)
        to_date = datetime.now()

        try:
            orders_response = await self.client.get_orders(
                account_number=self.account_number,
                from_entered_time=from_date,
                to_entered_time=to_date,
                status="WORKING",
            )

            orders = []
            # Convert Schwab orders to our Order format
            # This would need full implementation based on order structure
            logger.debug(f"Retrieved {len(orders_response)} open orders")
            
            # TODO: Parse Schwab orders into BaseOrder objects
            # This requires understanding the exact structure of orders_response

            return orders

        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    async def place_order(self, order: BaseOrder) -> str:
        """Place an order."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        try:
            # Convert our Order to Schwab order format
            schwab_order = self._convert_to_schwab_order(order)

            response = await self.client.place_order(
                account_number=self.account_number, order=schwab_order
            )

            # Extract order ID from response
            # The order ID is typically in the Location header
            order_id = None
            if hasattr(response, "headers"):
                location = response.headers.get("Location", "")
                if location:
                    order_id = location.split("/")[-1]

            if not order_id:
                order_id = "unknown"

            logger.info(f"Placed order: {order_id}")
            return order_id

        except Exception as e:
            logger.error(f"Failed to place order: {e}", exc_info=True)
            raise

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        try:
            await self.client.cancel_order(
                account_number=self.account_number, order_id=int(order_id)
            )
            logger.info(f"Cancelled order: {order_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def get_option_chain(
        self, symbol: str, expiration: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get option chain for a symbol."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        # Note: schwab-trader may not have full option chain support yet
        # This is a placeholder for when it's available
        try:
            # Option chain retrieval would go here
            logger.warning("Option chain retrieval not yet fully implemented")
            raise NotImplementedError("Option chain retrieval not yet implemented")
        except Exception as e:
            logger.error(f"Failed to get option chain for {symbol}: {e}")
            raise

    async def get_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market data for symbols."""
        if not self.client:
            raise RuntimeError("Not connected to Schwab")

        quotes = {}
        for symbol in symbols:
            try:
                # Note: Adjust method name based on actual schwab-trader API
                quote_response = await self.client.get_quote(symbol)

                quotes[symbol] = {
                    "last": Decimal(str(quote_response.quote.lastPrice)),
                    "bid": Decimal(str(quote_response.quote.bidPrice)),
                    "ask": Decimal(str(quote_response.quote.askPrice)),
                    "volume": int(quote_response.quote.totalVolume),
                    "close": Decimal(
                        str(getattr(quote_response.quote, "closePrice", 0))
                    ),
                }
            except Exception as e:
                logger.error(f"Failed to get quote for {symbol}: {e}")
                # Return None for failed quotes
                quotes[symbol] = None

        return quotes

    def _convert_to_schwab_order(self, order: BaseOrder):
        """Convert our Order format to Schwab order format."""
        # Build option symbol in Schwab format
        # Format: SYMBOL_MMDDYY[C|P]STRIKE
        # Example: AAPL_011525C00150000
        exp_str = order.contract.expiration.strftime("%m%d%y")
        
        # Strike must be in cents with 8 digits (e.g., 150.00 -> 00150000)
        strike_cents = int(float(order.contract.strike) * 1000)
        strike_str = f"{strike_cents:08d}"
        
        right = order.contract.right[0]  # C or P
        option_symbol = f"{order.contract.symbol}_{exp_str}{right}{strike_str}"

        # Map our action to Schwab instruction
        if order.action == "BUY":
            instruction = "BUY_TO_OPEN"
        else:  # SELL
            instruction = "SELL_TO_CLOSE"

        # Create Schwab order using library methods
        if order.order_type == "LIMIT" and order.limit_price:
            schwab_order = self.client.create_limit_order(
                symbol=option_symbol,
                quantity=order.quantity,
                limit_price=float(order.limit_price),
                instruction=instruction,
                description=f"{order.contract.symbol} Option",
            )
        else:  # MARKET
            schwab_order = self.client.create_market_order(
                symbol=option_symbol,
                quantity=order.quantity,
                instruction=instruction,
                description=f"{order.contract.symbol} Option",
            )

        return schwab_order
