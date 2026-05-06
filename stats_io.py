"""Shared helper for writing per-lead-window outputs.

Both ``compute_stats`` (per-stat accumulator dicts) and the downloaders'
``extract_forecast`` (per-lead arrays) iterate ``(start, end)`` windows and
write a combined output for each window where every lead is present. The
combine step differs (sum of accumulator dicts vs mean of arrays); the
iteration shape is shared.
"""

from __future__ import annotations

from typing import Callable, Iterable, TypeVar

T = TypeVar("T")


def write_lead_windows(
    per_lead: dict[int, T],
    windows: Iterable[tuple[int, int]],
    *,
    write_fn: Callable[[int, int, T], None],
    combine_fn: Callable[[list[T]], T],
) -> int:
    """For each complete window, combine its leads and call ``write_fn``.

    A window is "complete" iff every lead from ``start`` to ``end`` (inclusive)
    appears in ``per_lead``. Incomplete windows are silently skipped.

    Returns the number of windows written.
    """
    sorted_leads = sorted(per_lead)
    written = 0
    for start, end in windows:
        leads_in_window = [ld for ld in sorted_leads if start <= ld <= end]
        if len(leads_in_window) != end - start + 1:
            continue
        combined = combine_fn([per_lead[ld] for ld in leads_in_window])
        write_fn(start, end, combined)
        written += 1
    return written
