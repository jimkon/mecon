import sys
from unittest import mock

if "httpx" not in sys.modules:
    sys.modules["httpx"] = mock.MagicMock()
