from __future__ import annotations

import json
import unittest
from pathlib import Path

from data_agg.config import load_registry
from data_agg.models import ForwardPESnapshotRow
from data_agg.storage import JsonArtifactStore
from data_agg.universe import StaticRowsAdapter, UniverseService
from data_agg.valuations import ValuationService
from tests.helpers import cleanup_test_dir, make_test_dir


class ReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = make_test_dir("replay")
        self.store = JsonArtifactStore(self.temp_dir)
        self.universe_service = UniverseService(self.store)
        self.valuation_service = ValuationService(self.store)
        self.registry = load_registry(Path(__file__).resolve().parents[1] / "config" / "sources.json")
        self.bundle = self.registry["ivv_holdings"]

    def tearDown(self) -> None:
        cleanup_test_dir(self.temp_dir)

    def _fixture(self, name: str):
        path = Path(__file__).parent / "fixtures" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_replay_uses_point_in_time_membership(self) -> None:
        day_1 = self._fixture("ivv_snapshot_2026-04-19.json")
        day_2 = self._fixture("ivv_snapshot_2026-04-20.json")
        self.universe_service.refresh_universe(
            self.bundle,
            "2026-04-19",
            {
                "primary": StaticRowsAdapter(self.bundle.primary, day_1),
                "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
                "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
                "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, day_1),
            },
        )
        self.universe_service.refresh_universe(
            self.bundle,
            "2026-04-20",
            {
                "primary": StaticRowsAdapter(self.bundle.primary, day_2),
                "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
                "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
                "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, day_2),
            },
        )

        valuations = [
            ForwardPESnapshotRow(
                as_of_date="2026-04-19",
                member_id="cik:0000320193",
                ticker="AAPL",
                provider_id="fixture",
                forward_eps=8.4,
                forward_pe=25.0,
                stale_days=0,
                null_reason=None,
                methodology_tag="fixture",
                source_url="",
                quality_flag="accepted",
            ),
            ForwardPESnapshotRow(
                as_of_date="2026-04-19",
                member_id="cik:0000789019",
                ticker="MSFT",
                provider_id="fixture",
                forward_eps=11.5,
                forward_pe=30.0,
                stale_days=0,
                null_reason=None,
                methodology_tag="fixture",
                source_url="",
                quality_flag="accepted",
            ),
            ForwardPESnapshotRow(
                as_of_date="2026-04-19",
                member_id="cik:0001652044",
                ticker="GOOGL",
                provider_id="fixture",
                forward_eps=6.0,
                forward_pe=22.0,
                stale_days=0,
                null_reason=None,
                methodology_tag="fixture",
                source_url="",
                quality_flag="accepted",
            )
        ]
        self.valuation_service.snapshot_forward_pe("2026-04-19", valuations)

        aggregates = self.valuation_service.aggregate_forward_pe("2026-04-19", "ivv_holdings")
        broad = aggregates["__broad__"]

        self.assertIsNotNone(broad.forward_pe)
        self.assertEqual(broad.member_count, 3)
        self.assertEqual(broad.covered_member_count, 3)


if __name__ == "__main__":
    unittest.main()
