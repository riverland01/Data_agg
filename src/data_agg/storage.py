from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Iterable, Optional


TABLES = [
    "universe_snapshot",
    "universe_change_log",
    "security_master",
    "forward_pe_snapshot",
    "source_run_log",
    "chart_manifest",
]


class Store(ABC):
    @abstractmethod
    def append(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def read(self, table: str, predicate: Optional[Callable[[dict[str, Any]], bool]] = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def latest(
        self,
        table: str,
        key: str,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
    ) -> Optional[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def distinct(self, table: str, field: str) -> list[Any]:
        raise NotImplementedError


class JsonArtifactStore(Store):
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        for table in TABLES:
            path = self._path(table)
            if not path.exists():
                path.write_text("", encoding="utf-8")

    def _path(self, table: str) -> Path:
        return self.root / f"{table}.jsonl"

    def append(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        path = self._path(table)
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True, default=str))
                handle.write("\n")

    def read(self, table: str, predicate: Optional[Callable[[dict[str, Any]], bool]] = None) -> list[dict[str, Any]]:
        path = self._path(table)
        results: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if predicate is None or predicate(row):
                results.append(row)
        return results

    def latest(
        self,
        table: str,
        key: str,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
    ) -> Optional[dict[str, Any]]:
        rows = self.read(table, predicate)
        if not rows:
            return None
        return sorted(rows, key=lambda row: row.get(key) or "")[-1]

    def distinct(self, table: str, field: str) -> list[Any]:
        return sorted({row.get(field) for row in self.read(table) if field in row})


class DuckDBParquetStore(Store):
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "pipeline.duckdb"
        self._duckdb = self._load_duckdb()
        self._conn = self._duckdb.connect(str(self.db_path))
        self._init_tables()

    def _load_duckdb(self):
        import duckdb  # type: ignore

        return duckdb

    def _init_tables(self) -> None:
        for table in TABLES:
            self._conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (payload JSON)")

    def append(self, table: str, rows: Iterable[dict[str, Any]]) -> None:
        rows = list(rows)
        if not rows:
            return
        for row in rows:
            self._conn.execute(
                f"INSERT INTO {table} VALUES (?)",
                [json.dumps(row, ensure_ascii=True, sort_keys=True, default=str)],
            )
        self._checkpoint(table)

    def _checkpoint(self, table: str) -> None:
        parquet_path = self.root / f"{table}.parquet"
        self._conn.execute(
            f"COPY (SELECT payload FROM {table}) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
        )

    def read(self, table: str, predicate: Optional[Callable[[dict[str, Any]], bool]] = None) -> list[dict[str, Any]]:
        rows = [json.loads(row[0]) for row in self._conn.execute(f"SELECT payload FROM {table}").fetchall()]
        if predicate is None:
            return rows
        return [row for row in rows if predicate(row)]

    def latest(
        self,
        table: str,
        key: str,
        predicate: Optional[Callable[[dict[str, Any]], bool]] = None,
    ) -> Optional[dict[str, Any]]:
        rows = self.read(table, predicate)
        if not rows:
            return None
        return sorted(rows, key=lambda row: row.get(key) or "")[-1]

    def distinct(self, table: str, field: str) -> list[Any]:
        return sorted({row.get(field) for row in self.read(table) if field in row})


def create_store(root: str | Path = "data/state") -> Store:
    try:
        return DuckDBParquetStore(root)
    except Exception:
        return JsonArtifactStore(root)
