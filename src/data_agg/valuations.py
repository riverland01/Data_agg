from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Optional

from .models import ChartManifest, ForwardPESnapshotRow
from .storage import Store


@dataclass(slots=True)
class AggregateForwardPE:
    label: str
    forward_pe: Optional[float]
    covered_weight: float
    uncovered_weight: float
    member_count: int
    covered_member_count: int

    def as_record(self) -> dict:
        return asdict(self)


class ValuationService:
    def __init__(self, store: Store) -> None:
        self.store = store

    def snapshot_forward_pe(
        self,
        as_of_date: str,
        rows: list[ForwardPESnapshotRow],
    ) -> None:
        self.store.append("forward_pe_snapshot", [row.as_record() for row in rows])

    def aggregate_forward_pe(
        self,
        as_of_date: str,
        universe_id: str,
        universe_basis: str = "point_in_time",
    ) -> dict[str, AggregateForwardPE]:
        universe_rows = self.store.read(
            "universe_snapshot",
            lambda row: row["universe_id"] == universe_id and row["effective_date"] == as_of_date,
        )
        valuation_rows = {
            row["member_id"]: row
            for row in self.store.read(
                "forward_pe_snapshot",
                lambda row: row["as_of_date"] == as_of_date,
            )
        }
        buckets: dict[str, list[tuple[float, Optional[float]]]] = defaultdict(list)
        broad: list[tuple[float, Optional[float]]] = []

        for row in universe_rows:
            weight = float(row.get("weight") or 0.0)
            forward_pe = None
            valuation = valuation_rows.get(row["member_id"])
            if valuation is not None:
                forward_pe = valuation.get("forward_pe")
            sector = row.get("sector") or "Unknown"
            buckets[sector].append((weight, forward_pe))
            broad.append((weight, forward_pe))

        aggregates = {
            sector: self._aggregate_bucket(sector, values)
            for sector, values in buckets.items()
        }
        aggregates["__broad__"] = self._aggregate_bucket("S&P 500", broad)
        return aggregates

    def render_chart_manifest(
        self,
        chart_id: str,
        as_of_date: str,
        universe_basis: str,
        provenance_label: str,
        dataset_health: str,
        inputs: dict,
        output_path: str,
    ) -> ChartManifest:
        manifest = ChartManifest(
            chart_id=chart_id,
            as_of_date=as_of_date,
            universe_basis=universe_basis,
            provenance_label=provenance_label,
            dataset_health=dataset_health,
            inputs=inputs,
            output_path=output_path,
        )
        self.store.append("chart_manifest", [manifest.as_record()])
        return manifest

    def _aggregate_bucket(
        self,
        label: str,
        values: list[tuple[float, Optional[float]]],
    ) -> AggregateForwardPE:
        total_weight = sum(weight for weight, _ in values)
        covered = [(weight, pe) for weight, pe in values if pe and pe > 0]
        covered_weight = sum(weight for weight, _ in covered)
        uncovered_weight = max(total_weight - covered_weight, 0.0)
        if covered_weight <= 0:
            return AggregateForwardPE(
                label=label,
                forward_pe=None,
                covered_weight=0.0,
                uncovered_weight=uncovered_weight,
                member_count=len(values),
                covered_member_count=0,
            )
        normalized = [(weight / covered_weight, pe) for weight, pe in covered]
        earnings_yield = sum(weight * (1.0 / pe) for weight, pe in normalized)
        aggregate_pe = None if earnings_yield <= 0 else 1.0 / earnings_yield
        return AggregateForwardPE(
            label=label,
            forward_pe=aggregate_pe,
            covered_weight=covered_weight,
            uncovered_weight=uncovered_weight,
            member_count=len(values),
            covered_member_count=len(covered),
        )
