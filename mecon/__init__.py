"""mecon package."""
from pathlib import Path

__all__ = ["__version__"]

_VERSION_PATH = Path(__file__).resolve().parent.parent / "version.txt"
__version__ = _VERSION_PATH.read_text(encoding="utf-8").strip()
