"""Static operator UI (HTML/CSS/JS) packaged for ``control-plane`` to mount."""

from __future__ import annotations

from pathlib import Path


def static_dir() -> Path:
    """Directory containing ``index.html``, ``app.js``, ``styles.css``, ``favicon.svg``."""
    return Path(__file__).resolve().parent / "static"
