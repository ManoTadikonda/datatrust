"""Pytest bootstrap.

Keeps `src/` importable without requiring an editable install, and makes the
test suite deterministic by pinning the simulation seed and pointing the
warehouse at a temp directory.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Deterministic by default; individual tests may override.
os.environ.setdefault("DATATRUST_SIM_SEED", "20260101")
os.environ.setdefault("DATATRUST_LOG_FORMAT", "console")
os.environ.setdefault("DATATRUST_LOG_LEVEL", "WARNING")
