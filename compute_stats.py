#!/usr/bin/env python3
"""Compute every available statistical product through one orchestration DAG."""

from __future__ import annotations

import argparse

from model_registry import MODEL_REGISTRY
from stats_pipeline.orchestrator import STAGE_ORDER, run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=list(MODEL_REGISTRY), help="compute one model (default: all)")
    parser.add_argument("--force", action="store_true", help="invalidate and rebuild selected stages")
    parser.add_argument(
        "--only", action="append", choices=STAGE_ORDER, default=[],
        help="target a stage; repeat for multiple stages (default: all)",
    )
    args = parser.parse_args()
    summary = run_pipeline(
        models=[args.model] if args.model else None,
        only=set(args.only) if args.only else None,
        force=args.force,
    )
    print("\nRun summary")
    for result in summary.results:
        print(f"  {result.model:16} {result.stage:12} {result.status:9} {result.message}")


if __name__ == "__main__":
    main()
