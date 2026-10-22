"""CLI for the local tARget v4 extension.

  python -m atmospheric_rivers.tarv4 build-thresholds   # 1980-2019 climatology
  python -m atmospheric_rivers.tarv4 detect --start 2026-01-01 --end 2026-08-06
  python -m atmospheric_rivers.tarv4 validate --start 2025-10-01 --end 2025-12-31
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="atmospheric_rivers.tarv4")
    sub = parser.add_subparsers(dest="command", required=True)

    thresholds = sub.add_parser("build-thresholds")
    thresholds.add_argument("--workers", type=int, default=24)

    detect = sub.add_parser("detect")
    detect.add_argument("--start", required=True)
    detect.add_argument("--end", required=True)
    detect.add_argument("--workers", type=int, default=24)

    validate = sub.add_parser("validate")
    validate.add_argument("--start", required=True)
    validate.add_argument("--end", required=True)
    validate.add_argument("--workers", type=int, default=24)

    args = parser.parse_args(argv)
    if args.command == "build-thresholds":
        from atmospheric_rivers.tarv4 import climatology
        climatology.build(workers=args.workers)
        climatology.finalize()
        return 0
    if args.command == "detect":
        from atmospheric_rivers.tarv4 import extend
        extend.run(start=args.start, end=args.end, workers=args.workers)
        return 0
    if args.command == "validate":
        from atmospheric_rivers.tarv4 import validate
        validate.run(start=args.start, end=args.end, workers=args.workers)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
