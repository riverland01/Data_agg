from __future__ import annotations

import unittest

from data_agg.cli import _infer_universe_dataset, _resolve_refresh_dataset


class CliDatasetInferenceTests(unittest.TestCase):
    def test_infers_spdr_from_etf_symbol(self) -> None:
        rows = [
            {
                "ticker": "JPM",
                "issuer_name": "JPMorgan Chase & Co.",
                "sector": "Financials",
                "weight": 8.25,
                "etf_symbol": "XLF",
            }
        ]
        self.assertEqual(_infer_universe_dataset(rows), "spdr_sector_etf_holdings")

    def test_infers_ivv_from_source_url(self) -> None:
        rows = [
            {
                "ticker": "AAPL",
                "issuer_name": "Apple Inc.",
                "sector": "Information Technology",
                "weight": 7.1,
                "source_url": "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf",
            }
        ]
        self.assertEqual(_infer_universe_dataset(rows), "ivv_holdings")

    def test_explicit_dataset_wins(self) -> None:
        rows = [
            {
                "ticker": "JPM",
                "issuer_name": "JPMorgan Chase & Co.",
                "sector": "Financials",
                "weight": 8.25,
                "etf_symbol": "XLF",
            }
        ]
        self.assertEqual(_resolve_refresh_dataset("ivv_holdings", rows), "spdr_sector_etf_holdings")
        self.assertEqual(_resolve_refresh_dataset("spdr_sector_etf_holdings", rows), "spdr_sector_etf_holdings")


if __name__ == "__main__":
    unittest.main()
