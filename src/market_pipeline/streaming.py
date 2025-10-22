from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Protocol

from .data import TradeEvent


class EventSink(Protocol):
    def send(self, event: TradeEvent) -> None:
        ...


class LocalFileSink:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def send(self, event: TradeEvent) -> None:
        with self.output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_record()) + "\n")


class KafkaSink:
    def __init__(self, topic: str, bootstrap_servers: str = "localhost:9092") -> None:
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        try:
            from kafka import KafkaProducer
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Install kafka-python to use KafkaSink") from exc
        self._producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        )

    def send(self, event: TradeEvent) -> None:
        self._producer.send(self.topic, asdict(event))
        self._producer.flush()


class ConsoleSink:
    def __init__(self) -> None:
        self.count = 0

    def send(self, event: TradeEvent) -> None:
        self.count += 1
        print(json.dumps(event.to_record(), default=str))


def stream_events(events: Iterable[TradeEvent], sink: EventSink) -> int:
    count = 0
    for event in events:
        sink.send(event)
        count += 1
    return count
