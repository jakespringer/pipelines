"""Project configuration: ``Project.init`` and the layered-TOML ``Project.config``.

Layers (last wins): versioned ancestor/sub-project ``pipelines.toml`` < machine-wide
``<config_home>/pipelines/config.toml`` < per-project ``.../projects/<name>.toml`` <
explicit executor args. ``config_home`` honors ``XDG_CONFIG_HOME`` (default ``~/.config``).
See ``docs/10-config-project-packaging.md``.
"""

from __future__ import annotations

import os
from pathlib import Path

try:
    import tomllib  # Python >= 3.11
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


class ConfigKeyError(AttributeError):
    def __init__(self, key: str, system_path: Path):
        super().__init__(
            f"Project.config has no key {key!r}. Set it in a pipelines.toml "
            f"[config] table or in {system_path}")


class _ConfigView:
    def __init__(self, data: dict, system_path: Path):
        object.__setattr__(self, "_data", data)
        object.__setattr__(self, "_system_path", system_path)

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise ConfigKeyError(key, self._system_path) from None

    def get(self, key, default=None):
        """Optional read: the value for ``key`` or ``default`` if unset.

        Unlike attribute access, this never raises :class:`ConfigKeyError` — use it
        for keys a project may legitimately leave unconfigured."""
        return self._data.get(key, default)

    def to_dict(self) -> dict:
        return dict(self._data)


def _config_home() -> Path:
    return Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))


def _load_toml(path: Path) -> dict:
    if not path.is_file():
        return {}
    with path.open("rb") as fh:
        return tomllib.load(fh)


def _deep_merge(base: dict, overlay: dict) -> dict:
    out = dict(base)
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


class Project:
    name: str | None = None
    config: _ConfigView | None = None

    @classmethod
    def init(cls, name: str, *, from_file: str) -> None:
        merged: dict = {}

        # Versioned pipelines.toml, ancestor-most first so nearer files win.
        start = Path(from_file).resolve().parent
        toml_files = [d / "pipelines.toml" for d in [start, *start.parents]]
        for path in reversed([p for p in toml_files if p.is_file()]):
            merged = _deep_merge(merged, _load_toml(path).get("config", {}))

        # System overlays.
        home = _config_home() / "pipelines"
        system_path = home / "projects" / f"{name}.toml"
        merged = _deep_merge(merged, _load_toml(home / "config.toml").get("config", {}))
        merged = _deep_merge(merged, _load_toml(system_path).get("config", {}))

        cls.name = name
        cls.config = _ConfigView(merged, system_path)
