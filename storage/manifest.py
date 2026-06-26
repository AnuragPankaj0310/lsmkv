from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class Manifest:
    """Simple JSON manifest for active SSTables."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: list[dict[str, Any]] = []

    def load(self) -> None:
        if not self._path.exists():
            self._entries = []
            return

        with open(self._path, "r", encoding="utf-8") as f:
            data = json.load(f)

        entries = data.get("sstables", [])
        normalized: list[dict[str, Any]] = []
        for entry in entries:
            normalized.append(
                {
                    "path": str(entry.get("path", "")),
                    "level": int(entry.get("level", 0)),
                }
            )
        self._entries = normalized

    def save(self) -> None:
        tmp_path = self._path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({"sstables": self._entries}, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._path)

    def add_sstable(self, path: str | Path, level: int = 0) -> None:
        file_name = Path(path).name
        for entry in self._entries:
            if entry["path"] == file_name:
                entry["level"] = level
                return
        self._entries.append({"path": file_name, "level": level})

    def remove_sstable(self, path: str | Path) -> None:
        file_name = Path(path).name
        self._entries = [e for e in self._entries if e["path"] != file_name]

    def active_sstables(self) -> list[dict[str, Any]]:
        return list(self._entries)
