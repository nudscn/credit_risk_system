#!/usr/bin/env python3
"""Stage 1 initializer for the single master workbook.

Behavior:
- Creates the master workbook once.
- If the workbook already exists, it will not be overwritten.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = PROJECT_ROOT / "scripts" / "build_basic_data_workbook.py"


def normalize_project_id(project_id: str) -> str:
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", (project_id or "").strip())
    return text or "default_project"


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize single master workbook (stage 1)")
    parser.add_argument(
        "--project-id",
        default="default_project",
        help="Project identifier used for output isolation (default: default_project)",
    )
    parser.add_argument(
        "--master-workbook",
        default="",
        help="Master workbook path override; default is outputs/<project_id>/<project_id>_项目主文件.xlsx",
    )
    parser.add_argument(
        "--input-dir",
        default="",
        help="Input directory override; default scans inputs/<project_id>/ then inputs/",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild workbook even if it already exists",
    )
    args = parser.parse_args()
    project_id = normalize_project_id(args.project_id)
    if args.master_workbook:
        master_workbook = Path(args.master_workbook)
        if not master_workbook.is_absolute():
            master_workbook = PROJECT_ROOT / master_workbook
    else:
        master_workbook = PROJECT_ROOT / "outputs" / project_id / f"{project_id}_项目主文件.xlsx"

    cmd = [
        sys.executable,
        str(BUILD_SCRIPT),
        "--project-id",
        project_id,
        "--output",
        str(master_workbook),
    ]
    if not args.force:
        cmd.append("--skip-if-exists")
    if args.input_dir:
        cmd.extend(["--input-dir", args.input_dir])

    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        return proc.returncode

    print(f"Master workbook: {master_workbook}")
    if master_workbook.exists():
        print("Stage 1 ready: you can now edit this file manually, then run stage 2 validation repeatedly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
