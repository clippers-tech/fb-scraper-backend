"""
Logging utility for the Facebook Ad Scraper.
Writes to both console and scrape_log.txt.
"""

import os
import sys
from datetime import datetime


class ScrapeLogger:
    """Handles logging to console and file simultaneously."""

    def __init__(self, export_dir: str):
        self.export_dir = export_dir
        self.log_file = os.path.join(export_dir, "scrape_log.txt")
        os.makedirs(export_dir, exist_ok=True)
        # Initialize log file
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(f"=== Facebook Ad Scraper Log ===\n")
            f.write(f"Session started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'=' * 60}\n\n")

    def _timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write_to_file(self, message: str):
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"[{self._timestamp()}] {message}\n")

    def info(self, message: str):
        print(f"  {message}")
        self._write_to_file(f"[INFO] {message}")

    def success(self, message: str):
        print(f"  ✓ {message}")
        self._write_to_file(f"[SUCCESS] {message}")

    def warning(self, message: str):
        print(f"  ⚠ {message}")
        self._write_to_file(f"[WARNING] {message}")

    def error(self, message: str):
        print(f"  ✗ {message}")
        self._write_to_file(f"[ERROR] {message}")

    def progress(self, current: int, total: int, action: str, detail: str = ""):
        msg = f"[{current}/{total}] {action}"
        if detail:
            msg += f" — {detail}"
        print(f"\n{'─' * 60}")
        print(f"  {msg}")
        print(f"{'─' * 60}")
        self._write_to_file(f"[PROGRESS] {msg}")

    def separator(self):
        self._write_to_file("-" * 60)

    def finalize(self, summary_lines: list):
        self._write_to_file("")
        self._write_to_file("=" * 60)
        self._write_to_file("SESSION COMPLETE")
        for line in summary_lines:
            self._write_to_file(line)
        self._write_to_file(f"Session ended: {self._timestamp()}")
