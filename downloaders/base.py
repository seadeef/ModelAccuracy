"""Base downloader: shared session, parallel execution, progress reporting."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, TypeVar

import requests

T = TypeVar("T")


class BaseDownloader:
    """Shared init, session, and parallel run for model downloaders."""

    def __init__(
        self,
        output_dir: str | Path,
        *,
        max_workers: int = 6,
        max_retries: int = 3,
        timeout_seconds: int = 60,
        polite_delay_seconds: float = 0.0,
        base_url: str = "",
        user_agent: str = "BaseDownloader/1.0",
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = base_url
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.polite_delay_seconds = polite_delay_seconds
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

    def _status_key(self, status: str) -> str:
        """Map raw status string to a count key. Override in subclasses."""
        return status

    def _run_parallel(
        self,
        tasks: list[T],
        download_fn: Callable[[T], tuple[T, str]],
        description: str = "Download",
        progress_interval: int = 25,
    ) -> list[tuple[T, str]]:
        """Run download_fn on each task in parallel; aggregate counts, print progress; return (task, status) list."""
        if not tasks:
            print(f"{description}: no tasks")
            return []
        counts: dict[str, int] = defaultdict(int)
        results: list[tuple[T, str]] = []
        total = len(tasks)
        done = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_to_task = {pool.submit(download_fn, t): t for t in tasks}
            for fut in as_completed(future_to_task):
                task = future_to_task[fut]
                try:
                    _task, status = fut.result()
                    results.append((_task, status))
                except Exception as e:
                    status = f"failed: {e}"
                    results.append((task, status))
                key = self._status_key(status)
                counts[key] += 1
                done += 1
                if done % progress_interval == 0 or done == total:
                    parts = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
                    print(f"\r{description} Progress: {done}/{total} | {parts}", end="", flush=True)

        print()
        summary = " | ".join(f"{k}={counts[k]}" for k in sorted(counts))
        print(f"Done | {summary}")
        return results
