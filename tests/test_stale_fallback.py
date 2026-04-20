from __future__ import annotations

import json
import unittest
from pathlib import Path

from data_agg.config import load_registry
from data_agg.storage import JsonArtifactStore
from data_agg.universe import FailingAdapter, StaticRowsAdapter, UniverseService
from tests.helpers import cleanup_test_dir, make_test_dir


class StaleFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = make_test_dir("stale_fallback")
        self.store = JsonArtifactStore(self.temp_dir)
        self.service = UniverseService(self.store)
        self.registry = load_registry(Path(__file__).resolve().parents[1] / "config" / "sources.json")
        self.bundle = self.registry["ivv_holdings"]

    def tearDown(self) -> None:
        cleanup_test_dir(self.temp_dir)

    def _fixture(self):
        path = Path(__file__).parent / "fixtures" / "ivv_snapshot_2026-04-19.json"
        return json.loads(path.read_text(encoding="utf-8"))

    def test_uses_last_accepted_snapshot_when_sources_fail(self) -> None:
        first = self._fixture()
        adapters_ok = {
            "primary": StaticRowsAdapter(self.bundle.primary, first),
            "fallback_1": FailingAdapter(self.bundle.fallback_1),
            "fallback_2": FailingAdapter(self.bundle.fallback_2),
            "manual_recovery": FailingAdapter(self.bundle.manual_recovery),
        }
        self.service.refresh_universe(self.bundle, "2026-04-19", adapters_ok)

        adapters_fail = {
            "primary": FailingAdapter(self.bundle.primary),
            "fallback_1": FailingAdapter(self.bundle.fallback_1),
            "fallback_2": FailingAdapter(self.bundle.fallback_2),
            "manual_recovery": FailingAdapter(self.bundle.manual_recovery),
        }
        rows = self.service.refresh_universe(self.bundle, "2026-04-20", adapters_fail)

        self.assertTrue(rows)
        self.assertTrue(all(row.snapshot_type == "stale_fallback" for row in rows))
        self.assertTrue(all(row.quality_flag == "stale_universe_fallback" for row in rows))


if __name__ == "__main__":
    unittest.main()
