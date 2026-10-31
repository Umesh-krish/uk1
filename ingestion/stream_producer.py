"""
Kafka producer that replays a season's events as if matches were live.

Used to exercise the streaming path without waiting for a real fixture. Events
are emitted in minute order with a compressed clock, so a 90 minute match plays
out in roughly 90 seconds at the default speed.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def load_events(path: Path, match_limit: int | None) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    if match_limit:
        keep = []
        seen: set[str] = set()
        for row in rows:
            if row["match_id"] not in seen:
                if len(seen) >= match_limit:
                    continue
                seen.add(row["match_id"])
            keep.append(row)
        rows = keep

    rows.sort(key=lambda r: (r["match_id"], int(r["minute"])))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--events", default="data/landing/events/season=2025-26/events.csv")
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "match-events"))
    ap.add_argument("--matches", type=int, default=5, help="How many matches to replay")
    ap.add_argument("--speed", type=float, default=60.0, help="Clock multiplier; 60 = 1 match minute per second")
    ap.add_argument("--dry-run", action="store_true", help="Print to stdout instead of producing to Kafka")
    args = ap.parse_args()

    events = load_events(Path(args.events), args.matches)
    if not events:
        sys.exit(f"no events found at {args.events}")

    producer = None
    if not args.dry_run:
        try:
            from kafka import KafkaProducer
        except ImportError:
            sys.exit("kafka-python is not installed; run `pip install kafka-python` or pass --dry-run")

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode(),
            # Wait for all in-sync replicas. A dropped event here is a hole in
            # the in-play view that the batch path will not notice until the
            # nightly reconciliation test fires.
            acks="all",
            retries=5,
            linger_ms=50,
        )

    print(f"replaying {len(events)} events from {len({e['match_id'] for e in events})} matches")

    last_minute = 0
    sent = 0
    for row in events:
        minute = int(row["minute"])
        delay = max(0.0, (minute - last_minute) * 60.0 / args.speed)
        if delay:
            time.sleep(min(delay, 2.0))
        last_minute = minute

        payload = {
            "event_id": row["event_id"],
            "match_id": row["match_id"],
            "season": row["season"],
            "minute": minute,
            "team_code": row["team_code"],
            "event_type": row["event_type"],
            "player_id": row["player_id"],
            "emitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

        if args.dry_run:
            print(json.dumps(payload))
        else:
            # Keying by match_id keeps one match's events on one partition, so
            # per-match ordering is preserved without a global ordering penalty.
            producer.send(args.topic, key=row["match_id"], value=payload)
        sent += 1

    if producer:
        producer.flush()
        producer.close()

    print(f"sent {sent} events to {args.topic}")


if __name__ == "__main__":
    main()
