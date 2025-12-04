"""Base broker interface for ThetaGang."""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass


@dataclass
class Position:
    """Represents a position in an account."""

    symbol: str
    quantity: int
    average_cost: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal


@dataclass
class Contract:
    """Represents an option contract."""

    symbol: str
    strike: Decimal
    expiration: datetime
    right: str  # 'CALL' or 'PUT'
    multiplier: int = 100


@dataclass
class Order:
    """Represents an order."""

    contract: Contract
    action: str  # 'BUY' or 'SELL'
    quantity: int
    order_type: str  # 'MARKET', 'LIMIT', etc.
    limit_price: Optional[Decimal] = None


class BaseBroker(ABC):
    """Abstract base class for broker implementations."""

    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the broker."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the broker."""
        pass

    @abstractmethod
    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information including buying power, balances, etc."""
        pass

    @abstractmethod
    async def get_positions(self) -> List[Position]:
        """Get all current positions."""
        pass

    @abstractmethod
    async def get_open_orders(self) -> List[Order]:
        """Get all open orders."""
        pass

    @abstractmethod
    async def place_order(self, order: Order) -> str:
        """Place an order and return order ID."""
        pass

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        pass

    @abstractmethod
    async def get_option_chain(
        self, symbol: str, expiration: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get option chain for a symbol."""
        pass

    @abstractmethod
    async def get_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market data for symbols."""
        pass
