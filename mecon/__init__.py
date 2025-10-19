"""mecon package."""
from importlib import resources

__all__ = ["__version__"]


def _read_version() -> str:
    """Return the packaged project version string."""
    files = getattr(resources, "files", None)
    if files is not None:
        return files(__name__).joinpath("version.txt").read_text(encoding="utf-8").strip()
    # Fall back for Python versions without resources.files
    return resources.read_text(__name__, "version.txt", encoding="utf-8").strip()


__version__ = _read_version()
