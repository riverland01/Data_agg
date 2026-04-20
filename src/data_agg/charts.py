from __future__ import annotations

import json
from pathlib import Path

from .models import DatasetHealth
from .signals import SignalService
from .storage import Store
from .valuations import ValuationService


class ChartService:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.signals = SignalService(store)
        self.valuations = ValuationService(store)

    def render_forward_pe_chart_pack(
        self,
        as_of_date: str,
        root: str | Path = "data/charts",
        universe_basis: str = "point_in_time",
    ) -> Path:
        output_root = Path(root)
        output_root.mkdir(parents=True, exist_ok=True)
        signal = self.signals.compute_forward_pe_signal(
            as_of_date=as_of_date,
            universe_basis=universe_basis,
        )
        output_path = output_root / f"forward_pe_{as_of_date}.json"
        output_path.write_text(json.dumps(signal.payload, indent=2, ensure_ascii=True), encoding="utf-8")
        self.valuations.render_chart_manifest(
            chart_id="forward_pe_pack",
            as_of_date=as_of_date,
            universe_basis=universe_basis,
            provenance_label="Unofficial public snapshot" if signal.dataset_health != DatasetHealth.GREEN.value else "Public free aggregate",
            dataset_health=signal.dataset_health,
            inputs={"signal_id": signal.signal_id, "universe_id": "ivv_holdings"},
            output_path=str(output_path),
        )
        return output_path
