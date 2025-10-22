from __future__ import annotations

from pathlib import Path

from market_pipeline.demo import replay_to_local_file


if __name__ == "__main__":
    replay_to_local_file(Path("data/raw/live_replay.jsonl"), total_events=1000, events_per_second=500)
