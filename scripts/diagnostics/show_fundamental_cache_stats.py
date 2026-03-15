"""Print summary stats for Git-stored fundamentals metadata."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def main() -> None:
    metadata_path = Path("data/fundamentals_cache/metadata.json")
    if not metadata_path.exists():
        print("No metadata file found")
        return

    print("Latest updates:")
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        updates = [(ticker, data["last_updated"]) for ticker, data in metadata.items()]
        updates.sort(key=lambda x: x[1], reverse=True)

        print(f"Total tracked: {len(updates)} stocks")
        print("\nLast 10 updates:")
        for ticker, timestamp in updates[:10]:
            dt = datetime.fromisoformat(timestamp)
            print(f"  {ticker}: {dt.strftime('%Y-%m-%d %H:%M')}")
    except Exception as exc:  # noqa: BLE001 - diagnostic script should never hard-fail workflow
        print(f"Could not read metadata: {exc}")


if __name__ == "__main__":
    main()
