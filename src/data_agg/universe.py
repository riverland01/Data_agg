from __future__ import annotations

from dataclasses import replace
from datetime import date
from typing import Any, Optional

from .models import (
    ChangeType,
    DatasetHealth,
    SecurityMasterRow,
    SnapshotType,
    SourceBundle,
    SourceEndpoint,
    SourceRunLogRow,
    SourceRunStatus,
    UniverseBasis,
    UniverseChangeLogRow,
    UniverseSnapshotRow,
)
from .storage import Store
from .utils import coerce_float, normalized_name, schema_hash, stable_member_id, utc_now


class SourceFetchError(RuntimeError):
    """Raised when a source adapter fails to fetch data."""


class SourceAdapter:
    endpoint: SourceEndpoint

    def __init__(self, endpoint: SourceEndpoint) -> None:
        self.endpoint = endpoint

    def fetch(self, as_of_date: str) -> list[dict[str, Any]]:
        raise NotImplementedError


class StaticRowsAdapter(SourceAdapter):
    def __init__(self, endpoint: SourceEndpoint, rows: list[dict[str, Any]]) -> None:
        super().__init__(endpoint)
        self.rows = rows

    def fetch(self, as_of_date: str) -> list[dict[str, Any]]:
        return list(self.rows)


class FailingAdapter(SourceAdapter):
    def fetch(self, as_of_date: str) -> list[dict[str, Any]]:
        raise SourceFetchError(f"{self.endpoint.provider_id} unavailable")


class UniverseValidator:
    def __init__(self, bundle: SourceBundle) -> None:
        self.bundle = bundle

    def validate(self, rows: list[dict[str, Any]]) -> tuple[bool, str]:
        if not rows:
            return False, "empty_payload"
        missing = [
            column
            for column in self.bundle.required_columns
            if any(column not in row for row in rows)
        ]
        if missing:
            return False, f"missing_columns:{','.join(sorted(set(missing)))}"
        return True, "ok"


class UniverseChangeDetector:
    def diff(
        self,
        universe_id: str,
        effective_date: str,
        previous_rows: list[UniverseSnapshotRow],
        current_rows: list[UniverseSnapshotRow],
    ) -> list[UniverseChangeLogRow]:
        previous_by_member = {row.member_id: row for row in previous_rows}
        current_by_member = {row.member_id: row for row in current_rows}
        previous_by_cik = {row.cik: row for row in previous_rows if row.cik}
        changes: list[UniverseChangeLogRow] = []

        for member_id, current in current_by_member.items():
            previous = previous_by_member.get(member_id)
            if previous is None:
                matched_previous = previous_by_cik.get(current.cik) if current.cik else None
                change_type = ChangeType.ADD
                confidence = 1.0
                notes = "new member"
                if matched_previous and matched_previous.member_id != member_id:
                    change_type = ChangeType.CORPORATE_ACTION
                    confidence = 0.8
                    notes = "identifier changed with matching CIK"
                changes.append(
                    UniverseChangeLogRow(
                        universe_id=universe_id,
                        detected_at=utc_now(),
                        effective_date=effective_date,
                        change_type=change_type.value,
                        member_id=member_id,
                        old_value={},
                        new_value=current.as_record(),
                        confidence=confidence,
                        notes=notes,
                    )
                )
                continue

            change_type, notes, confidence = self._classify_update(previous, current)
            if change_type is not None:
                changes.append(
                    UniverseChangeLogRow(
                        universe_id=universe_id,
                        detected_at=utc_now(),
                        effective_date=effective_date,
                        change_type=change_type.value,
                        member_id=member_id,
                        old_value=previous.as_record(),
                        new_value=current.as_record(),
                        confidence=confidence,
                        notes=notes,
                    )
                )

        for member_id, previous in previous_by_member.items():
            if member_id in current_by_member:
                continue
            change_type = ChangeType.REMOVE
            notes = "member removed"
            confidence = 1.0
            for current in current_rows:
                if previous.cik and previous.cik == current.cik and previous.ticker != current.ticker:
                    change_type = ChangeType.TICKER_CHANGE
                    notes = "ticker updated for same CIK"
                    confidence = 0.95
                    break
                if normalized_name(previous.issuer_name) == normalized_name(current.issuer_name):
                    change_type = ChangeType.CORPORATE_ACTION
                    notes = "issuer lineage suggests corporate action"
                    confidence = 0.6
                    break
            changes.append(
                UniverseChangeLogRow(
                    universe_id=universe_id,
                    detected_at=utc_now(),
                    effective_date=effective_date,
                    change_type=change_type.value,
                    member_id=member_id,
                    old_value=previous.as_record(),
                    new_value={},
                    confidence=confidence,
                    notes=notes,
                )
            )

        return changes

    def _classify_update(
        self,
        previous: UniverseSnapshotRow,
        current: UniverseSnapshotRow,
    ) -> tuple[Optional[ChangeType], str, float]:
        if previous.ticker != current.ticker:
            return ChangeType.TICKER_CHANGE, "ticker changed with stable member id", 0.95
        if normalized_name(previous.issuer_name) != normalized_name(current.issuer_name):
            return ChangeType.NAME_CHANGE, "issuer name changed", 0.9
        if previous.sector != current.sector:
            return ChangeType.SECTOR_RECLASS, "sector reclassification", 0.95
        if (previous.weight or 0.0) != (current.weight or 0.0):
            return ChangeType.WEIGHT_CHANGE_ONLY, "weight changed", 0.99
        return None, "", 0.0


class UniverseService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.detector = UniverseChangeDetector()

    def refresh_universe(
        self,
        bundle: SourceBundle,
        as_of_date: str,
        adapters: dict[str, SourceAdapter],
    ) -> list[UniverseSnapshotRow]:
        validator = UniverseValidator(bundle)
        errors: list[str] = []

        for slot_name in ("primary", "fallback_1", "fallback_2", "manual_recovery"):
            endpoint = getattr(bundle, slot_name)
            adapter = adapters.get(slot_name)
            if adapter is None:
                errors.append(f"{slot_name}:missing_adapter")
                continue
            try:
                raw_rows = adapter.fetch(as_of_date)
            except Exception as exc:
                errors.append(f"{slot_name}:{exc.__class__.__name__}")
                self._log_source_run(
                    dataset_id=bundle.dataset_id,
                    provider_id=endpoint.provider_id,
                    status=SourceRunStatus.FAILED.value,
                    rows=[],
                    notes=str(exc),
                    health=DatasetHealth.YELLOW.value,
                )
                continue

            valid, reason = validator.validate(raw_rows)
            if not valid:
                errors.append(f"{slot_name}:{reason}")
                health = DatasetHealth.RED.value if slot_name == "primary" else DatasetHealth.YELLOW.value
                self._log_source_run(
                    dataset_id=bundle.dataset_id,
                    provider_id=endpoint.provider_id,
                    status=SourceRunStatus.SCHEMA_DRIFT.value if reason.startswith("missing_columns") else SourceRunStatus.EMPTY_PAYLOAD.value,
                    rows=raw_rows,
                    notes=reason,
                    health=health,
                )
                continue

            snapshot_rows = self._materialize_snapshot_rows(
                universe_id=bundle.dataset_id,
                effective_date=as_of_date,
                provider=endpoint,
                rows=raw_rows,
                snapshot_type=SnapshotType.ACCEPTED.value,
            )
            self._accept_snapshot(bundle.dataset_id, as_of_date, snapshot_rows)
            self._update_security_master(snapshot_rows)
            self._log_source_run(
                dataset_id=bundle.dataset_id,
                provider_id=endpoint.provider_id,
                status=SourceRunStatus.SUCCESS.value,
                rows=raw_rows,
                notes=f"accepted_from:{slot_name}",
                health=DatasetHealth.GREEN.value if slot_name == "primary" else DatasetHealth.YELLOW.value,
            )
            return snapshot_rows

        fallback = self._latest_accepted_snapshot(bundle.dataset_id)
        if not fallback:
            raise SourceFetchError(f"no accepted snapshot available for {bundle.dataset_id}; errors={errors}")
        latest_effective_date = date.fromisoformat(fallback[0].effective_date)
        requested_date = date.fromisoformat(as_of_date)
        staleness_days = (requested_date - latest_effective_date).days
        if staleness_days > bundle.staleness_tolerance_days:
            raise SourceFetchError(
                f"latest accepted snapshot for {bundle.dataset_id} is {staleness_days} days stale; "
                f"tolerance={bundle.staleness_tolerance_days}; errors={errors}"
            )

        stale_rows = [
            replace(
                row,
                effective_date=as_of_date,
                snapshot_type=SnapshotType.STALE_FALLBACK.value,
                quality_flag="stale_universe_fallback",
                universe_basis=UniverseBasis.STALE_FALLBACK.value,
            )
            for row in fallback
        ]
        self.store.append("universe_snapshot", [row.as_record() for row in stale_rows])
        self._log_source_run(
            dataset_id=bundle.dataset_id,
            provider_id="carry_forward",
            status=SourceRunStatus.STALE_DATA.value,
            rows=[row.as_record() for row in stale_rows],
            notes=f"used_last_accepted_snapshot:{staleness_days}_days_stale",
            health=DatasetHealth.YELLOW.value,
        )
        return stale_rows

    def diff_universe(self, universe_id: str, effective_date: str) -> list[UniverseChangeLogRow]:
        current = self._snapshot_for_date(universe_id, effective_date)
        previous = self._previous_accepted_snapshot(universe_id, effective_date)
        if not current:
            return []
        changes = self.detector.diff(universe_id, effective_date, previous, current)
        if changes:
            self.store.append("universe_change_log", [row.as_record() for row in changes])
        return changes

    def snapshot_history(self, universe_id: str) -> list[UniverseSnapshotRow]:
        rows = self.store.read("universe_snapshot", lambda row: row["universe_id"] == universe_id)
        return [UniverseSnapshotRow(**row) for row in rows]

    def latest_basis(self, universe_id: str) -> str:
        row = self.store.latest(
            "universe_snapshot",
            key="effective_date",
            predicate=lambda record: record["universe_id"] == universe_id,
        )
        if not row:
            return UniverseBasis.POINT_IN_TIME.value
        return row.get("universe_basis", UniverseBasis.POINT_IN_TIME.value)

    def _accept_snapshot(
        self,
        universe_id: str,
        effective_date: str,
        rows: list[UniverseSnapshotRow],
    ) -> None:
        self.store.append("universe_snapshot", [row.as_record() for row in rows])
        previous = self._previous_accepted_snapshot(universe_id, effective_date)
        changes = self.detector.diff(universe_id, effective_date, previous, rows)
        if changes:
            self.store.append("universe_change_log", [row.as_record() for row in changes])
            self._append_security_status_events(changes, effective_date)

    def _materialize_snapshot_rows(
        self,
        universe_id: str,
        effective_date: str,
        provider: SourceEndpoint,
        rows: list[dict[str, Any]],
        snapshot_type: str,
    ) -> list[UniverseSnapshotRow]:
        materialized: list[UniverseSnapshotRow] = []
        for row in rows:
            member_id = stable_member_id(
                ticker=row.get("ticker"),
                cik=row.get("cik"),
                issuer_name=row.get("issuer_name"),
                raw_identifier=row.get("raw_identifier"),
            )
            materialized.append(
                UniverseSnapshotRow(
                    universe_id=universe_id,
                    effective_date=effective_date,
                    member_id=member_id,
                    ticker=(row.get("ticker") or "").upper(),
                    cik=row.get("cik"),
                    issuer_name=row.get("issuer_name") or row.get("name") or "",
                    sector=row.get("sector"),
                    weight=coerce_float(row.get("weight")),
                    snapshot_type=snapshot_type,
                    source_url=provider.url,
                    quality_flag="accepted" if snapshot_type == SnapshotType.ACCEPTED.value else "stale_universe_fallback",
                    provider_id=provider.provider_id,
                    raw_identifier=row.get("raw_identifier"),
                )
            )
        return materialized

    def _update_security_master(self, rows: list[UniverseSnapshotRow]) -> None:
        existing = {row["member_id"]: row for row in self.store.read("security_master")}
        to_append: list[SecurityMasterRow] = []
        for row in rows:
            record = existing.get(row.member_id)
            if record is None:
                to_append.append(
                    SecurityMasterRow(
                        member_id=row.member_id,
                        ticker=row.ticker,
                        cik=row.cik,
                        issuer_name=row.issuer_name,
                        valid_from=row.effective_date,
                        valid_to=None,
                        status="active",
                        sector=row.sector,
                    )
                )
                continue
            if record["ticker"] != row.ticker or record["issuer_name"] != row.issuer_name or record.get("sector") != row.sector:
                to_append.append(
                    SecurityMasterRow(
                        member_id=row.member_id,
                        ticker=row.ticker,
                        cik=row.cik,
                        issuer_name=row.issuer_name,
                        valid_from=row.effective_date,
                        valid_to=None,
                        status="active",
                        sector=row.sector,
                    )
                )
        if to_append:
            self.store.append("security_master", [row.as_record() for row in to_append])

    def _append_security_status_events(
        self,
        changes: list[UniverseChangeLogRow],
        effective_date: str,
    ) -> None:
        rows: list[SecurityMasterRow] = []
        for change in changes:
            if change.change_type != ChangeType.REMOVE.value:
                continue
            old = change.old_value
            rows.append(
                SecurityMasterRow(
                    member_id=change.member_id,
                    ticker=old.get("ticker", ""),
                    cik=old.get("cik"),
                    issuer_name=old.get("issuer_name", ""),
                    valid_from=effective_date,
                    valid_to=effective_date,
                    status="inactive",
                    sector=old.get("sector"),
                )
            )
        if rows:
            self.store.append("security_master", [row.as_record() for row in rows])

    def _latest_accepted_snapshot(self, universe_id: str) -> list[UniverseSnapshotRow]:
        rows = self.store.read(
            "universe_snapshot",
            lambda row: row["universe_id"] == universe_id and row["snapshot_type"] == SnapshotType.ACCEPTED.value,
        )
        if not rows:
            return []
        latest_date = max(row["effective_date"] for row in rows)
        return [UniverseSnapshotRow(**row) for row in rows if row["effective_date"] == latest_date]

    def _previous_accepted_snapshot(self, universe_id: str, effective_date: str) -> list[UniverseSnapshotRow]:
        rows = self.store.read(
            "universe_snapshot",
            lambda row: row["universe_id"] == universe_id
            and row["snapshot_type"] == SnapshotType.ACCEPTED.value
            and row["effective_date"] < effective_date,
        )
        if not rows:
            return []
        latest_date = max(row["effective_date"] for row in rows)
        return [UniverseSnapshotRow(**row) for row in rows if row["effective_date"] == latest_date]

    def _snapshot_for_date(self, universe_id: str, effective_date: str) -> list[UniverseSnapshotRow]:
        rows = self.store.read(
            "universe_snapshot",
            lambda row: row["universe_id"] == universe_id and row["effective_date"] == effective_date,
        )
        return [UniverseSnapshotRow(**row) for row in rows]

    def _log_source_run(
        self,
        dataset_id: str,
        provider_id: str,
        status: str,
        rows: list[dict[str, Any]],
        notes: str,
        health: str,
    ) -> None:
        row = SourceRunLogRow(
            dataset_id=dataset_id,
            run_ts=utc_now(),
            provider_id=provider_id,
            status=status,
            error_class=None if status == SourceRunStatus.SUCCESS.value else status,
            row_count=len(rows),
            schema_hash=schema_hash(rows),
            notes=notes,
            health=health,
        )
        self.store.append("source_run_log", [row.as_record()])
