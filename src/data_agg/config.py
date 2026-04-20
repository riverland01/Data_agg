from __future__ import annotations

import json
from pathlib import Path

from .models import SourceBundle, SourceEndpoint


def _endpoint_from_dict(payload: dict) -> SourceEndpoint:
    return SourceEndpoint(
        provider_id=payload["provider_id"],
        source_type=payload["source_type"],
        parser_type=payload["parser_type"],
        url=payload["url"],
        freshness_sla_hours=int(payload["freshness_sla_hours"]),
        stability_score=float(payload["stability_score"]),
        constituent_defining=bool(payload["constituent_defining"]),
        legal_notes=payload["legal_notes"],
    )


def load_registry(path: str | Path = "config/sources.json") -> dict[str, SourceBundle]:
    source_path = Path(path)
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    bundles: dict[str, SourceBundle] = {}
    for dataset_id, dataset_payload in payload["datasets"].items():
        sources = dataset_payload["sources"]
        bundles[dataset_id] = SourceBundle(
            dataset_id=dataset_payload["dataset_id"],
            description=dataset_payload["description"],
            staleness_tolerance_days=int(dataset_payload["staleness_tolerance_days"]),
            required_columns=list(dataset_payload["required_columns"]),
            primary=_endpoint_from_dict(sources["primary"]),
            fallback_1=_endpoint_from_dict(sources["fallback_1"]),
            fallback_2=_endpoint_from_dict(sources["fallback_2"]),
            manual_recovery=_endpoint_from_dict(sources["manual_recovery"]),
        )
    return bundles
