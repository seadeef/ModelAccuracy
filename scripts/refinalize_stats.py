#!/usr/bin/env python3
"""Re-run plugin.finalize() on every saved lead_*.npz, in place.

When a plugin's finalize logic changes (e.g., a new mask, a different
formula) the stored ``value`` arrays go stale, but the raw accumulator
fields (``sum_*``, ``sample_count``, …) are still valid — re-finalizing is
seconds, while re-accumulating is many minutes.

Usage:
    python3 scripts/refinalize_stats.py             # all stats, all models
    python3 scripts/refinalize_stats.py --stat nmad nrmse
    python3 scripts/refinalize_stats.py --model nbm
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from statistics_plugins.registry import STATISTICS_BY_NAME

DEFAULT_ROOT = _PROJECT_ROOT / "stats_output"


def refinalize_npz(npz_path: Path, plugin) -> None:
    with np.load(npz_path) as loaded:
        accumulator = {k: loaded[k] for k in loaded.files if k != "value"}
    outputs = plugin.finalize(accumulator)
    np.savez_compressed(npz_path, **outputs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stat", nargs="+", default=None,
                        help="Stat names to refinalize (default: all)")
    parser.add_argument("--model", nargs="+", default=None,
                        help="Model keys to refinalize (default: all under stats_output/)")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()

    stat_names = args.stat or list(STATISTICS_BY_NAME)
    unknown = [s for s in stat_names if s not in STATISTICS_BY_NAME]
    if unknown:
        print(f"ERROR: unknown stat name(s): {unknown}", file=sys.stderr)
        return 1

    if not args.root.is_dir():
        print(f"ERROR: {args.root} is not a directory", file=sys.stderr)
        return 1

    model_dirs = sorted(p for p in args.root.iterdir() if p.is_dir())
    if args.model:
        wanted = set(args.model)
        model_dirs = [p for p in model_dirs if p.name in wanted]

    total = 0
    for model_dir in model_dirs:
        for stat_name in stat_names:
            stat_dir = model_dir / stat_name
            if not stat_dir.is_dir():
                continue
            plugin = STATISTICS_BY_NAME[stat_name]
            paths = sorted(stat_dir.rglob("lead_*.npz"))
            for p in paths:
                refinalize_npz(p, plugin)
                total += 1
            if paths:
                print(f"  {model_dir.name}/{stat_name}: refinalized {len(paths)} files")

    print(f"==> done, refinalized {total} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
