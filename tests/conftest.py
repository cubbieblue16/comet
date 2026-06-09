"""Test environment bootstrap.

Must run before any `comet.*` import: comet.core.models builds the global
Database instance at import time from DATABASE_TYPE/DATABASE_PATH, so the
sqlite-backed tests need these pointed at a throwaway location first.
"""

import os
import tempfile

_TMP_DIR = tempfile.mkdtemp(prefix="comet-tests-")
os.environ["DATABASE_TYPE"] = "sqlite"
os.environ["DATABASE_PATH"] = os.path.join(_TMP_DIR, "comet-test.db")
