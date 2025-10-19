from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from typing import Iterable, Iterator

import pandas as pd

from .config import StreamConfig


@dataclass(frozen=True)
class TradeEvent:
    symbol: str
    timestamp: datetime
    price: float
    size: float
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float

    def to_record(self) -> dict[str, object]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat()
        return record


class MarketDataGenerator:
    def __init__(self, config: StreamConfig | None = None) -> None:
        self.config = config or StreamConfig()
        self.random = Random(self.config.seed)
        self.base_prices = {
            symbol: 100.0 + idx * 25.0 for idx, symbol in enumerate(self.config.symbols)
        }

    def generate_event(self, symbol: str, timestamp: datetime) -> TradeEvent:
        base_price = self.base_prices[symbol]
        shock = self.random.gauss(0, 0.06)
        drift = self.random.gauss(0, 0.01)
        price = max(0.01, base_price * (1.0 + shock + drift))
        self.base_prices[symbol] = 0.999 * base_price + 0.001 * price
        spread = max(0.01, price * 0.0005)
        bid_price = round(price - spread / 2, 4)
        ask_price = round(price + spread / 2, 4)
        size = max(1.0, self.random.expovariate(1 / 12.0))
        bid_size = max(1.0, self.random.expovariate(1 / 15.0))
        ask_size = max(1.0, self.random.expovariate(1 / 15.0))
        return TradeEvent(
            symbol=symbol,
            timestamp=timestamp,
            price=round(price, 4),
            size=round(size, 4),
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=round(bid_size, 4),
            ask_size=round(ask_size, 4),
        )

    def generate_frame(self, total_events: int, start: datetime | None = None) -> pd.DataFrame:
        start_time = start or datetime.now(tz=UTC)
        rows: list[dict[str, object]] = []
        for index in range(total_events):
            symbol = self.config.symbols[index % len(self.config.symbols)]
            timestamp = start_time + timedelta(milliseconds=index)
            rows.append(self.generate_event(symbol, timestamp).to_record())
        frame = pd.DataFrame(rows)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame

    def replay(self, total_events: int) -> Iterator[TradeEvent]:
        start_time = datetime.now(tz=UTC)
        for index in range(total_events):
            symbol = self.config.symbols[index % len(self.config.symbols)]
            timestamp = start_time + timedelta(milliseconds=index)
            yield self.generate_event(symbol, timestamp)


def frame_from_events(events: Iterable[TradeEvent]) -> pd.DataFrame:
    frame = pd.DataFrame([event.to_record() for event in events])
    if not frame.empty:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame
