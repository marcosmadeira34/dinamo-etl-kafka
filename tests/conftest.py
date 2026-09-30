"""
tests/conftest.py

Garante que a raiz do projeto e `src/` estejam no sys.path para que os testes
possam importar pacotes do projeto sem exigir instalação via pip install -e.
O repositório mistura dois estilos de import: `from src.x import ...`
(precisa da raiz do projeto no path) e `from pre_diagnostic_engine.x import ...`
(precisa de src/ no path).
"""
import sys
from pathlib import Path

ROOT_PATH = str(Path(__file__).resolve().parent.parent)
SRC_PATH = str(Path(ROOT_PATH) / "src")

for path in (ROOT_PATH, SRC_PATH):
    if path not in sys.path:
        sys.path.insert(0, path)
