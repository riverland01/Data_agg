from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from typing import Any, Iterable, Mapping, Optional


def schema_hash(rows: Iterable[Mapping[str, Any]]) -> str:
    columns: set[str] = set()
    for row in rows:
        columns.update(row.keys())
    payload = "|".join(sorted(columns))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def stable_member_id(
    ticker: Optional[str],
    cik: Optional[str],
    issuer_name: Optional[str],
    raw_identifier: Optional[str] = None,
) -> str:
    if cik:
        return f"cik:{str(cik).zfill(10)}"
    if raw_identifier:
        return f"raw:{raw_identifier}"
    if ticker:
        return f"ticker:{ticker.upper()}"
    cleaned = re.sub(r"[^a-z0-9]+", "-", (issuer_name or "").strip().lower()).strip("-")
    return f"name:{cleaned or 'unknown'}"


def normalized_name(value: Optional[str]) -> str:
    text = (value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text).strip()
    return re.sub(r"\s+", " ", text)


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def iso_date(value: Optional[str] = None) -> str:
    if value:
        return date.fromisoformat(value).isoformat()
    return date.today().isoformat()


def coerce_float(value: Any) -> Optional[float]:
    if value in (None, "", "NA", "N/A", "--", "."):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
