#!/usr/bin/env python3
"""Stage 1 initializer for the single master workbook.

Behavior:
- Creates the master workbook once.
- If the workbook already exists, it will not be overwritten.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path("/Users/sijia/Documents/credit_risk_system")
BUILD_SCRIPT = PROJECT_ROOT / "scripts" / "build_basic_data_workbook.py"
MASTER_WORKBOOK = PROJECT_ROOT / "outputs" / "十堰城运_基础数据_主文件.xlsx"


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize single master workbook (stage 1)")
    parser.add_argument("--mock-full", action="store_true", help="Use synthetic full data for structure testing")
    args = parser.parse_args()

    cmd = [
        sys.executable,
        str(BUILD_SCRIPT),
        "--output",
        str(MASTER_WORKBOOK),
        "--skip-if-exists",
    ]
    if args.mock_full:
        cmd.append("--mock-full")

    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        return proc.returncode

    print(f"Master workbook: {MASTER_WORKBOOK}")
    if MASTER_WORKBOOK.exists():
        print("Stage 1 ready: you can now edit this file manually, then run stage 2 validation repeatedly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
