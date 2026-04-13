"""Root conftest — make hyphenated sub-packages importable with underscore names."""

import pathlib
import sys
import types

_root = pathlib.Path(__file__).parent

# Map hyphenated directories to importable package names
_mappings = {
    "kafka-es-consumer": "kafka_es_consumer",
    "watch-history-consumer": "watch_history_consumer",
    "catalog-admin": "catalog_admin",
}

for dir_name, pkg_name in _mappings.items():
    pkg_dir = _root / dir_name
    if pkg_dir.is_dir() and pkg_name not in sys.modules:
        mod = types.ModuleType(pkg_name)
        mod.__path__ = [str(pkg_dir)]
        mod.__package__ = pkg_name
        sys.modules[pkg_name] = mod

        # Register sub-packages (e.g. catalog_admin.tests, catalog_admin.src)
        for sub in pkg_dir.iterdir():
            if sub.is_dir() and (sub / "__init__.py").exists():
                sub_name = f"{pkg_name}.{sub.name}"
                if sub_name not in sys.modules:
                    sub_mod = types.ModuleType(sub_name)
                    sub_mod.__path__ = [str(sub)]
                    sub_mod.__package__ = sub_name
                    sys.modules[sub_name] = sub_mod
