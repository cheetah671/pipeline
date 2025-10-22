from __future__ import annotations

import json
import time
from pathlib import Path

from .config import StreamConfig
from .data import MarketDataGenerator
from .streaming import ConsoleSink, KafkaSink, LocalFileSink


def replay_events(total_events: int, sink, events_per_second: int = 1000) -> int:
    generator = MarketDataGenerator(StreamConfig(events_per_second=events_per_second))
    start = time.perf_counter()
    count = 0
    sleep_interval = 1.0 / max(events_per_second, 1)
    for event in generator.replay(total_events):
        sink.send(event)
        count += 1
        time.sleep(sleep_interval)
    elapsed = time.perf_counter() - start
    print({"events": count, "seconds": elapsed, "events_per_second": count / max(elapsed, 1e-9)})
    return count


def replay_to_local_file(output_path: Path, total_events: int, events_per_second: int = 1000) -> int:
    sink = LocalFileSink(output_path)
    return replay_events(total_events, sink, events_per_second)


def replay_to_kafka(topic: str, total_events: int, events_per_second: int = 1000, bootstrap_servers: str = "localhost:9092") -> int:
    sink = KafkaSink(topic=topic, bootstrap_servers=bootstrap_servers)
    return replay_events(total_events, sink, events_per_second)


def log_local_file_events(input_path: Path) -> int:
    count = 0
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            print(payload)
            count += 1
    return count


def consume_from_kafka(topic: str, bootstrap_servers: str = "localhost:9092", max_messages: int = 1000) -> int:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install kafka-python to use consume_from_kafka") from exc

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    count = 0
    for message in consumer:
        print(message.value)
        count += 1
        if count >= max_messages:
            break
    return count


def console_replay(total_events: int, events_per_second: int = 1000) -> int:
    sink = ConsoleSink()
    return replay_events(total_events, sink, events_per_second)
