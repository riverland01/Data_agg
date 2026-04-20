from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Dict, Optional


class SourceType(str, Enum):
    OFFICIAL_FREE = "official_free"
    STRUCTURED_PUBLIC = "structured_public"
    UNOFFICIAL_PUBLIC = "unofficial_public"
    VALIDATION_ONLY = "validation_only"
    MANUAL_RECOVERY = "manual_recovery"


class ParserType(str, Enum):
    JSON = "json"
    CSV = "csv"
    HTML_TABLE = "html_table"
    WRAPPER = "wrapper"


class DatasetHealth(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class UniverseBasis(str, Enum):
    POINT_IN_TIME = "point_in_time"
    LATEST_AVAILABLE = "latest_available"
    ROLLING_PROXY = "rolling_proxy"
    CURRENT_RETROSPECTIVE = "current_universe_retrospective"
    STALE_FALLBACK = "stale_universe_fallback"


class SnapshotType(str, Enum):
    ACCEPTED = "accepted"
    STALE_FALLBACK = "stale_fallback"
    VALIDATION = "validation"


class ChangeType(str, Enum):
    ADD = "add"
    REMOVE = "remove"
    WEIGHT_CHANGE_ONLY = "weight_change_only"
    TICKER_CHANGE = "ticker_change"
    NAME_CHANGE = "name_change"
    SECTOR_RECLASS = "sector_reclass"
    CORPORATE_ACTION = "corporate_action_split_merge_spinoff"


class SourceRunStatus(str, Enum):
    SUCCESS = "success"
    SCHEMA_DRIFT = "schema_drift"
    EMPTY_PAYLOAD = "empty_payload"
    STALE_DATA = "stale_data"
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(slots=True)
class SourceEndpoint:
    provider_id: str
    source_type: str
    parser_type: str
    url: str
    freshness_sla_hours: int
    stability_score: float
    constituent_defining: bool
    legal_notes: str

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SourceBundle:
    dataset_id: str
    description: str
    staleness_tolerance_days: int
    required_columns: list[str]
    primary: SourceEndpoint
    fallback_1: SourceEndpoint
    fallback_2: SourceEndpoint
    manual_recovery: SourceEndpoint


@dataclass(slots=True)
class UniverseSnapshotRow:
    universe_id: str
    effective_date: str
    member_id: str
    ticker: str
    cik: Optional[str]
    issuer_name: str
    sector: Optional[str]
    weight: Optional[float]
    snapshot_type: str
    source_url: str
    quality_flag: str
    provider_id: str
    universe_basis: str = UniverseBasis.POINT_IN_TIME.value
    raw_identifier: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class UniverseChangeLogRow:
    universe_id: str
    detected_at: str
    effective_date: str
    change_type: str
    member_id: str
    old_value: Dict[str, Any]
    new_value: Dict[str, Any]
    confidence: float
    notes: str

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecurityMasterRow:
    member_id: str
    ticker: str
    cik: Optional[str]
    issuer_name: str
    valid_from: str
    valid_to: Optional[str]
    status: str
    cusip: Optional[str] = None
    isin: Optional[str] = None
    sector: Optional[str] = None
    updated_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ForwardPESnapshotRow:
    as_of_date: str
    member_id: str
    ticker: str
    provider_id: str
    forward_eps: Optional[float]
    forward_pe: Optional[float]
    stale_days: int
    null_reason: Optional[str]
    methodology_tag: str
    source_url: str
    quality_flag: str
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SourceRunLogRow:
    dataset_id: str
    run_ts: str
    provider_id: str
    status: str
    error_class: Optional[str]
    row_count: int
    schema_hash: str
    notes: str
    health: str

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ChartManifest:
    chart_id: str
    as_of_date: str
    universe_basis: str
    provenance_label: str
    dataset_health: str
    inputs: Dict[str, Any]
    output_path: str

    def as_record(self) -> Dict[str, Any]:
        return asdict(self)


def normalize_date(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value
