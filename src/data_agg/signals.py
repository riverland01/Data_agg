from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import DatasetHealth
from .storage import Store
from .valuations import ValuationService


@dataclass(slots=True)
class SignalResult:
    signal_id: str
    as_of_date: str
    universe_basis: str
    payload: dict[str, Any]
    dataset_health: str


class SignalService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.valuations = ValuationService(store)

    def compute_forward_pe_signal(
        self,
        as_of_date: str,
        universe_id: str = "ivv_holdings",
        universe_basis: str = "point_in_time",
    ) -> SignalResult:
        aggregates = self.valuations.aggregate_forward_pe(
            as_of_date=as_of_date,
            universe_id=universe_id,
            universe_basis=universe_basis,
        )
        broad = aggregates.get("__broad__")
        health = DatasetHealth.GREEN.value if broad and broad.forward_pe is not None else DatasetHealth.YELLOW.value
        payload = {
            "broad": broad.as_record() if broad else None,
            "sectors": {key: value.as_record() for key, value in aggregates.items() if key != "__broad__"},
        }
        return SignalResult(
            signal_id="forward_pe_aggregate",
            as_of_date=as_of_date,
            universe_basis=universe_basis,
            payload=payload,
            dataset_health=health,
        )
