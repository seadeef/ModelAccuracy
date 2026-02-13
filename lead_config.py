#!/usr/bin/env python3
from __future__ import annotations

LEAD_DAYS_MIN = 1
LEAD_DAYS_MAX = 14

# Forecast hours derived from lead-day bounds.
FORECAST_HOURS = [day * 24 for day in range(LEAD_DAYS_MIN, LEAD_DAYS_MAX + 1)]
