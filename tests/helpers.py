from __future__ import annotations

import shutil
import uuid
from pathlib import Path


def make_test_dir(label: str) -> Path:
    root = Path(__file__).resolve().parents[1] / ".tmp_tests"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{label}_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def cleanup_test_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
