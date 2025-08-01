import sys
import importlib.util
from pathlib import Path

STUB_DIR = Path(__file__).resolve().parent / 'stubs'
PROJECT_ROOT = STUB_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def _load_stub(name):
    file = STUB_DIR / name / '__init__.py'
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    sys.modules[name] = module

if 'pandas' not in sys.modules:
    _load_stub('pandas')
if 'numpy' not in sys.modules:
    _load_stub('numpy')
if 'aiomysql' not in sys.modules:
    _load_stub('aiomysql')
