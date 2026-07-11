from __future__ import annotations

import ast
import pathlib
import subprocess
import sys

_SRC_ROOT = pathlib.Path(__file__).resolve().parent.parent / "src"
_PKG_ROOT = _SRC_ROOT / "dramatiq_monitor"

_ALLOWED_ROOTS = {"starlette", "jinja2", "redis", "uvicorn", "dramatiq_monitor"}
_CONTRIB_EXTRA = {"dramatiq"}

# Python 3.9/3.9 don't have sys.stdlib_module_names; fall back to a set that
# covers everything this codebase actually imports.
_STDLIB_FALLBACK = {
    "__future__", "abc", "argparse", "ast", "asyncio", "base64", "collections",
    "contextlib", "dataclasses", "datetime", "functools", "hashlib", "hmac",
    "importlib", "inspect", "io", "itertools", "json", "os", "pathlib", "re",
    "secrets", "shutil", "socket", "subprocess", "sys", "tempfile", "time",
    "types", "typing", "unittest", "urllib", "uuid",
}


def _stdlib_names() -> set:
    names = getattr(sys, "stdlib_module_names", None)
    if names:
        return set(names)
    return _STDLIB_FALLBACK


def _iter_py_files():
    for path in sorted(_PKG_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        yield path


def _top_level_import_roots(path: pathlib.Path):
    tree = ast.parse(path.read_text(), filename=str(path))
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level and node.module is None:
                continue  # `from . import x` — relative, already in-package
            if node.level:
                continue  # relative import, e.g. `from .foo import bar`
            if node.module:
                roots.add(node.module.split(".")[0])
    return roots


def test_import_roots_are_within_allowlist():
    stdlib = _stdlib_names()

    violations = []
    for path in _iter_py_files():
        allowed = _ALLOWED_ROOTS | stdlib
        if path.relative_to(_PKG_ROOT).parts[0] == "contrib":
            allowed = allowed | _CONTRIB_EXTRA

        roots = _top_level_import_roots(path)
        bad = {r for r in roots if r not in allowed}
        if bad:
            violations.append((str(path.relative_to(_SRC_ROOT)), bad))

    assert not violations, f"disallowed imports found: {violations}"


def test_only_contrib_worker_meta_imports_dramatiq():
    for path in _iter_py_files():
        roots = _top_level_import_roots(path)
        if "dramatiq" in roots:
            rel = path.relative_to(_PKG_ROOT)
            assert rel == pathlib.Path("contrib") / "worker_meta.py", (
                f"unexpected dramatiq import in {rel}"
            )


def test_importing_app_does_not_import_dramatiq():
    code = (
        "import sys\n"
        "import dramatiq_monitor.app\n"
        "assert 'dramatiq' not in sys.modules, sys.modules.keys()\n"
        "assert not any(m == 'dramatiq' or m.startswith('dramatiq.') for m in sys.modules)\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(_SRC_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout
