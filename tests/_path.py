"""Make ``scripts/`` importable from the tests without installing anything."""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
SAMPLES_DIR = REPO_ROOT / "samples" / "2020Mar"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
