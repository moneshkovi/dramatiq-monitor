from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__", "create_app"]


def __getattr__(name: str):
    if name == "create_app":
        from .app import create_app

        return create_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
