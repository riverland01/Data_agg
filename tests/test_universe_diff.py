from __future__ import annotations

import json
import unittest
from pathlib import Path

from data_agg.config import load_registry
from data_agg.storage import JsonArtifactStore
from data_agg.universe import StaticRowsAdapter, UniverseService
from tests.helpers import cleanup_test_dir, make_test_dir


class UniverseDiffTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = make_test_dir("universe_diff")
        self.store = JsonArtifactStore(self.temp_dir)
        self.service = UniverseService(self.store)
        self.registry = load_registry(Path(__file__).resolve().parents[1] / "config" / "sources.json")
        self.bundle = self.registry["ivv_holdings"]

    def tearDown(self) -> None:
        cleanup_test_dir(self.temp_dir)

    def _fixture(self, name: str):
        path = Path(__file__).parent / "fixtures" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_detects_add_remove_weight_and_sector_changes(self) -> None:
        first = self._fixture("ivv_snapshot_2026-04-19.json")
        second = self._fixture("ivv_snapshot_2026-04-20.json")

        adapters_day_1 = {
            "primary": StaticRowsAdapter(self.bundle.primary, first),
            "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
            "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
            "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, first),
        }
        adapters_day_2 = {
            "primary": StaticRowsAdapter(self.bundle.primary, second),
            "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
            "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
            "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, second),
        }

        self.service.refresh_universe(self.bundle, "2026-04-19", adapters_day_1)
        self.service.refresh_universe(self.bundle, "2026-04-20", adapters_day_2)

        changes = self.service.diff_universe("ivv_holdings", "2026-04-20")
        change_types = {row.change_type for row in changes}

        self.assertIn("add", change_types)
        self.assertIn("remove", change_types)
        self.assertIn("weight_change_only", change_types)
        self.assertIn("sector_reclass", change_types)

    def test_ticker_change_preserves_identifier_lineage(self) -> None:
        first = [
            {
                "ticker": "FB",
                "cik": "0001326801",
                "issuer_name": "Meta Platforms, Inc.",
                "sector": "Communication Services",
                "weight": 2.5,
            }
        ]
        second = [
            {
                "ticker": "META",
                "cik": "0001326801",
                "issuer_name": "Meta Platforms, Inc.",
                "sector": "Communication Services",
                "weight": 2.6,
            }
        ]

        adapters_day_1 = {
            "primary": StaticRowsAdapter(self.bundle.primary, first),
            "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
            "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
            "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, first),
        }
        adapters_day_2 = {
            "primary": StaticRowsAdapter(self.bundle.primary, second),
            "fallback_1": StaticRowsAdapter(self.bundle.fallback_1, []),
            "fallback_2": StaticRowsAdapter(self.bundle.fallback_2, []),
            "manual_recovery": StaticRowsAdapter(self.bundle.manual_recovery, second),
        }

        self.service.refresh_universe(self.bundle, "2026-04-21", adapters_day_1)
        self.service.refresh_universe(self.bundle, "2026-04-22", adapters_day_2)

        changes = self.service.diff_universe("ivv_holdings", "2026-04-22")
        ticker_changes = [row for row in changes if row.change_type == "ticker_change"]

        self.assertTrue(ticker_changes)
        self.assertEqual(ticker_changes[0].member_id, "cik:0001326801")


if __name__ == "__main__":
    unittest.main()
