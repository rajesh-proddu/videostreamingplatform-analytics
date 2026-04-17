"""Tests for catalog-admin CLI commands: compact, expire-snapshots, describe, main."""

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# Stub out pyiceberg (same pattern as test_admin.py)
_pyiceberg_modules = {
    "pyiceberg": ModuleType("pyiceberg"),
    "pyiceberg.catalog": ModuleType("pyiceberg.catalog"),
    "pyiceberg.partitioning": ModuleType("pyiceberg.partitioning"),
    "pyiceberg.schema": ModuleType("pyiceberg.schema"),
    "pyiceberg.transforms": ModuleType("pyiceberg.transforms"),
    "pyiceberg.types": ModuleType("pyiceberg.types"),
}
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
    cmd_compact,
    cmd_describe,
    cmd_expire_snapshots,
    main,
)


class TestCmdCompact:
    def test_compact_calls_table_compact(self):
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.compact.return_value = {"rewritten": 3}
        mock_catalog.load_table.return_value = mock_table

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            args.table = "analytics.watch_history"
            cmd_compact(args)

            mock_catalog.load_table.assert_called_once_with("analytics.watch_history")
            mock_table.compact.assert_called_once()


class TestCmdExpireSnapshots:
    def test_expire_calls_manage_snapshots(self):
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            args.table = "analytics.watch_history"
            cmd_expire_snapshots(args)

            mock_catalog.load_table.assert_called_once_with("analytics.watch_history")
            mock_table.manage_snapshots.return_value.expire_snapshots.assert_called_once()


class TestCmdDescribe:
    def test_describe_loads_and_inspects_table(self):
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_table.metadata.location = "s3://bucket/table"
        mock_table.metadata.properties = {"write.format.default": "parquet"}
        mock_table.metadata.snapshots = [MagicMock()]
        mock_catalog.load_table.return_value = mock_table

        with patch("catalog_admin.admin.get_catalog", return_value=mock_catalog):
            args = MagicMock()
            args.table = "analytics.watch_history"
            cmd_describe(args)

            mock_catalog.load_table.assert_called_once_with("analytics.watch_history")
            mock_table.schema.assert_called()
            mock_table.spec.assert_called()


class TestMainCLI:
    @patch("catalog_admin.admin.cmd_create_tables")
    @patch("catalog_admin.admin.get_catalog")
    def test_main_create_tables(self, mock_get_catalog, mock_cmd, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py", "create-tables"])
        main()
        mock_cmd.assert_called_once()

    @patch("catalog_admin.admin.cmd_list_tables")
    @patch("catalog_admin.admin.get_catalog")
    def test_main_list(self, mock_get_catalog, mock_cmd, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py", "list"])
        main()
        mock_cmd.assert_called_once()

    @patch("catalog_admin.admin.cmd_describe")
    @patch("catalog_admin.admin.get_catalog")
    def test_main_describe(self, mock_get_catalog, mock_cmd, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py", "describe", "analytics.watch_history"])
        main()
        mock_cmd.assert_called_once()
        assert mock_cmd.call_args[0][0].table == "analytics.watch_history"

    @patch("catalog_admin.admin.cmd_compact")
    @patch("catalog_admin.admin.get_catalog")
    def test_main_compact(self, mock_get_catalog, mock_cmd, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py", "compact", "analytics.watch_history"])
        main()
        mock_cmd.assert_called_once()

    @patch("catalog_admin.admin.cmd_expire_snapshots")
    @patch("catalog_admin.admin.get_catalog")
    def test_main_expire_snapshots(self, mock_get_catalog, mock_cmd, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py", "expire-snapshots", "analytics.watch_history"])
        main()
        mock_cmd.assert_called_once()

    def test_main_missing_command_exits(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["admin.py"])
        with pytest.raises(SystemExit):
            main()
