from pathlib import Path
from pkgutil import extend_path
import sys

SRC = Path(__file__).resolve().parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

__path__ = extend_path(__path__, __name__)
