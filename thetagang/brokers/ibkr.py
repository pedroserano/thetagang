"""IBKR broker implementation for ThetaGang.

This wraps the existing ib_async functionality to match the BaseBroker interface.
"""
import logging
from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime

from ib_async import IB, Stock, Option, Order as IBOrder

from .base import BaseBroker, Position, Contract, Order as BaseOrder

logger = logging.getLogger(__name__)


class IBKRBroker(BaseBroker):
    """IBKR broker implementation wrapping ib_async."""

    def __init__(self, ib: IB, config: Dict[str, Any]):
        """
        Initialize IBKR broker.

        Args:
            ib: Connected IB instance
            config: Configuration dictionary
        """
        self.ib = ib
        self.config = config
        self.account_number = config["account"]["ibkr"]["number"]

    async def connect(self) -> bool:
        """
        Connect to IBKR.

        Note: The IB connection should already be established before
        creating this broker instance.
        """
        if self.ib.isConnected():
            logger.info("IBKR connection already established")
            return True
        else:
            logger.error("IBKR not connected")
            return False

    async def disconnect(self) -> None:
        """Disconnect from IBKR."""
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info("Disconnected from IBKR")

    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information from IBKR."""
        account_values = await self.ib.accountSummaryAsync()

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

    async def get_positions(self) -> List[Position]:
        """Get all current positions from IBKR."""
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

        logger.debug(f"Retrieved {len(positions)} positions from IBKR")
        return positions

    async def get_open_orders(self) -> List[BaseOrder]:
        """Get all open orders from IBKR."""
        trades = self.ib.openTrades()

        orders = []
        for trade in trades:
            # Parse IBKR trade into our Order format
            # This is simplified - full implementation would parse all order types
            try:
                if isinstance(trade.contract, Option):
                    contract = Contract(
                        symbol=trade.contract.symbol,
                        strike=Decimal(str(trade.contract.strike)),
                        expiration=datetime.strptime(
                            trade.contract.lastTradeDateOrContractMonth, "%Y%m%d"
                        ),
                        right="CALL" if trade.contract.right == "C" else "PUT",
                        multiplier=int(trade.contract.multiplier),
                    )

                    order = BaseOrder(
                        contract=contract,
                        action=trade.order.action,
                        quantity=int(trade.order.totalQuantity),
                        order_type=trade.order.orderType,
                        limit_price=Decimal(str(trade.order.lmtPrice))
                        if hasattr(trade.order, "lmtPrice") and trade.order.lmtPrice
                        else None,
                    )
                    orders.append(order)
            except Exception as e:
                logger.error(f"Error parsing IBKR order: {e}")
                continue

        logger.debug(f"Retrieved {len(orders)} open orders from IBKR")
        return orders

    async def place_order(self, order: BaseOrder) -> str:
        """Place an order with IBKR."""
        # Create IBKR option contract
        ib_contract = Option(
            symbol=order.contract.symbol,
            lastTradeDateOrContractMonth=order.contract.expiration.strftime("%Y%m%d"),
            strike=float(order.contract.strike),
            right=order.contract.right[0],  # 'C' or 'P'
            exchange="SMART",
            currency="USD",
            multiplier=str(order.contract.multiplier),
        )

        # Qualify the contract
        await self.ib.qualifyContractsAsync(ib_contract)

        # Create IBKR order
        ib_order = IBOrder()
        ib_order.action = order.action
        ib_order.totalQuantity = order.quantity
        ib_order.orderType = order.order_type

        if order.order_type == "LIMIT" and order.limit_price:
            ib_order.lmtPrice = float(order.limit_price)

        # Apply order configuration from config
        orders_config = self.config.get("orders", {})
        
        # Set exchange
        if orders_config.get("exchange"):
            ib_contract.exchange = orders_config["exchange"]
        
        # Set algorithm strategy if specified
        algo_config = orders_config.get("algo", {})
        if algo_config.get("strategy"):
            ib_order.algoStrategy = algo_config["strategy"]
            ib_order.algoParams = []
            
            # Add algo params
            params = algo_config.get("params", {})
            for key, value in params.items():
                from ib_async import TagValue
                ib_order.algoParams.append(TagValue(key, str(value)))

        # Place the order
        trade = self.ib.placeOrder(ib_contract, ib_order)

        # Wait for order to be submitted
        await self.ib.sleepAsync(1)

        order_id = str(trade.order.orderId)
        logger.info(f"Placed IBKR order {order_id}")
        
        return order_id

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order with IBKR."""
        try:
            # Find the order
            trades = self.ib.trades()
            for trade in trades:
                if str(trade.order.orderId) == order_id:
                    self.ib.cancelOrder(trade.order)
                    logger.info(f"Cancelled IBKR order {order_id}")
                    return True

            logger.warning(f"IBKR order {order_id} not found")
            return False

        except Exception as e:
            logger.error(f"Error cancelling IBKR order {order_id}: {e}")
            return False

    async def get_option_chain(
        self, symbol: str, expiration: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get option chain from IBKR.
        
        Note: This is a complex operation with IBKR and requires
        multiple API calls to get strikes, expirations, etc.
        """
        # Create underlying contract
        stock = Stock(symbol, "SMART", currency="USD")
        await self.ib.qualifyContractsAsync(stock)

        # Request option chain data
        chains = await self.ib.reqSecDefOptParamsAsync(
            stock.symbol, "", stock.secType, stock.conId
        )

        if not chains:
            logger.warning(f"No option chains found for {symbol}")
            return {}

        # Parse chain data
        chain_data = {
            "symbol": symbol,
            "expirations": [],
            "strikes": [],
        }

        for chain in chains:
            chain_data["expirations"].extend(chain.expirations)
            chain_data["strikes"].extend(chain.strikes)

        # Remove duplicates and sort
        chain_data["expirations"] = sorted(set(chain_data["expirations"]))
        chain_data["strikes"] = sorted(set(chain_data["strikes"]))

        logger.debug(
            f"Retrieved option chain for {symbol}: "
            f"{len(chain_data['expirations'])} expirations, "
            f"{len(chain_data['strikes'])} strikes"
        )

        return chain_data

    async def get_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market data from IBKR."""
        quotes = {}

        for symbol in symbols:
            try:
                # Create stock contract
                contract = Stock(symbol, "SMART", currency="USD")
                await self.ib.qualifyContractsAsync(contract)

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

                # Cancel market data subscription
                self.ib.cancelMktData(contract)

            except Exception as e:
                logger.error(f"Failed to get IBKR market data for {symbol}: {e}")
                quotes[symbol] = None

        return quotes

    def wait_for_market_price(self, ticker, timeout: int = 60):
        """
        Wait for market price data to be available.
        
        This is a helper method for IBKR-specific functionality.
        """
        import time
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if ticker.last and ticker.last > 0:
                return True
            
            self.ib.sleep(0.1)
        
        logger.warning(f"Timeout waiting for market price for {ticker.contract.symbol}")
        return False
