"""Tests for the Iceberg catalog-admin CLI."""

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

# Stub out pyiceberg so the module can be imported without the real package.
_pyiceberg_modules = {
    "pyiceberg": ModuleType("pyiceberg"),
    "pyiceberg.catalog": ModuleType("pyiceberg.catalog"),
    "pyiceberg.partitioning": ModuleType("pyiceberg.partitioning"),
    "pyiceberg.schema": ModuleType("pyiceberg.schema"),
    "pyiceberg.transforms": ModuleType("pyiceberg.transforms"),
    "pyiceberg.types": ModuleType("pyiceberg.types"),
}

# Provide the names admin.py imports
_pyiceberg_modules["pyiceberg.catalog"].load_catalog = MagicMock()
_pyiceberg_modules["pyiceberg.partitioning"].PartitionField = MagicMock()
_pyiceberg_modules["pyiceberg.partitioning"].PartitionSpec = MagicMock()
_pyiceberg_modules["pyiceberg.schema"].Schema = MagicMock()
_pyiceberg_modules["pyiceberg.transforms"].DayTransform = MagicMock()

for name in ("LongType", "NestedField", "StringType", "TimestamptzType"):
    setattr(_pyiceberg_modules["pyiceberg.types"], name, MagicMock())

for mod_name, mod in _pyiceberg_modules.items():
    sys.modules.setdefault(mod_name, mod)

from catalog_admin.admin import (  # noqa: E402
    TABLES,
    cmd_create_tables,
    cmd_list_tables,
    get_catalog,
)


class TestTablesConfig:
    def test_tables_config_has_watch_history(self):
        assert "analytics.watch_history" in TABLES
        defn = TABLES["analytics.watch_history"]
        assert "schema" in defn
        assert "partition_spec" in defn
        assert "properties" in defn


class TestGetCatalog:
    def test_get_catalog_local(self, monkeypatch):
        monkeypatch.setenv("GLUE_ENDPOINT", "http://localhost:4566")
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:4566")
        monkeypatch.setenv("AWS_REGION", "us-east-1")

        mock_load = MagicMock(return_value=MagicMock())
        with patch("catalog_admin.admin.load_catalog", mock_load):
            get_catalog()
            mock_load.assert_called_once()
            call_kwargs = mock_load.call_args
            assert call_kwargs.kwargs["glue.endpoint"] == "http://localhost:4566"

    def test_get_catalog_aws(self, monkeypatch):
        monkeypatch.delenv("GLUE_ENDPOINT", raising=False)
        monkeypatch.delenv("S3_ENDPOINT", raising=False)

        mock_load = MagicMock(return_value=MagicMock())
        with patch("catalog_admin.admin.load_catalog", mock_load):
            get_catalog()
            mock_load.assert_called_once()
            call_kwargs = mock_load.call_args
            assert "glue.endpoint" not in call_kwargs.kwargs


class TestCmdCreateTables:
    def test_cmd_create_tables_new(self, monkeypatch):
        mock_catalog = MagicMock()
        # create_namespace succeeds (new namespace)
        mock_catalog.create_namespace.return_value = None
        # load_table raises → table doesn't exist yet
        mock_catalog.load_table.side_effect = Exception("NoSuchTableError")

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            cmd_create_tables(args)
            mock_catalog.create_table.assert_called_once()

    def test_cmd_create_tables_existing(self, monkeypatch):
        mock_catalog = MagicMock()
        mock_catalog.create_namespace.side_effect = Exception("already exists")
        # load_table succeeds → table exists
        mock_catalog.load_table.return_value = MagicMock()

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            cmd_create_tables(args)
            mock_catalog.create_table.assert_not_called()


class TestCmdListTables:
    def test_cmd_list_tables(self):
        mock_catalog = MagicMock()
        mock_catalog.list_namespaces.return_value = [("analytics",)]

        mock_table = MagicMock()
        mock_table.schema.return_value.fields = [MagicMock()] * 7
        mock_table.metadata.snapshots = []

        mock_catalog.list_tables.return_value = [("analytics", "watch_history")]
        mock_catalog.load_table.return_value = mock_table

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            cmd_list_tables(args)
            mock_catalog.list_namespaces.assert_called_once()
            mock_catalog.list_tables.assert_called_once_with("analytics")
