#!/usr/bin/env python3
from __future__ import annotations

from typing import Iterable

LEAD_WINDOWS: list[tuple[int, int]] = [
    (1, 7),
    (7, 14),
    (1, 10),
]


def window_to_key(start: int, end: int) -> str:
    return f"{start}_{end}"


def normalize_lead_key(lead: str | int) -> str:
    if isinstance(lead, int):
        return str(lead)
    cleaned = lead.strip().replace("-", "_")
    parts = cleaned.split("_")
    if len(parts) == 1 and parts[0].isdigit():
        return parts[0]
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"{int(parts[0])}_{int(parts[1])}"
    raise ValueError(f"Invalid lead key: {lead}")


def build_lead_options(min_lead: int, max_lead: int) -> list[dict[str, str]]:
    options: list[dict[str, str]] = [
        {"key": str(lead), "label": str(lead)}
        for lead in range(min_lead, max_lead + 1)
    ]
    for start, end in LEAD_WINDOWS:
        if start < min_lead or end > max_lead:
            continue
        options.append(
            {
                "key": window_to_key(start, end),
                "label": f"{start}-{end} avg",
            }
        )
    return options


def is_window_key(lead_key: str) -> bool:
    return "_" in lead_key


def iter_window_leads(start: int, end: int) -> Iterable[int]:
    return range(start, end + 1)
