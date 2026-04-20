from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from data_agg.holdings import HoldingsExporter, HoldingsHtmlParser
from tests.helpers import cleanup_test_dir, make_test_dir


IVV_HTML = """
<html>
  <body>
    <table>
      <tr><th>Ticker</th><th>Issuer Name</th><th>Sector</th><th>Weight</th><th>CIK</th></tr>
      <tr><td>AAPL</td><td>Apple Inc.</td><td>Information Technology</td><td>7.10%</td><td>0000320193</td></tr>
      <tr><td>MSFT</td><td>Microsoft Corporation</td><td>Information Technology</td><td>6.90%</td><td>0000789019</td></tr>
    </table>
  </body>
</html>
"""

SPDR_HTML = """
<html>
  <body>
    <table>
      <tr><th>Symbol</th><th>Company Name</th><th>Index Weight</th></tr>
      <tr><td>JPM</td><td>JPMorgan Chase &amp; Co.</td><td>8.25%</td></tr>
      <tr><td>BAC</td><td>Bank of America Corp</td><td>6.50%</td></tr>
    </table>
  </body>
</html>
"""

ISHARES_CSV = """Fund Holdings as of,2026-04-20
Ticker,Name,Sector,Weight (%)
AAPL,Apple Inc.,Information Technology,7.10
MSFT,Microsoft Corporation,Information Technology,6.90
"""


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, mapping: dict[str, bytes | str]) -> None:
        self.mapping = mapping

    def get(self, url: str, headers=None, timeout: int = 30):  # noqa: ANN001
        if url not in self.mapping:
            raise AssertionError(f"unexpected URL: {url}")
        payload = self.mapping[url]
        response = FakeResponse(payload if isinstance(payload, str) else "")
        response.content = payload if isinstance(payload, bytes) else payload.encode("utf-8")
        return response


class HoldingsParserTests(unittest.TestCase):
    def test_parser_normalizes_ivv_table(self) -> None:
        parser = HoldingsHtmlParser()
        holdings = parser.parse(IVV_HTML, source_url="https://example.com/ivv")

        self.assertEqual(len(holdings), 2)
        self.assertEqual(holdings[0].ticker, "AAPL")
        self.assertEqual(holdings[0].cik, "0000320193")
        self.assertEqual(holdings[0].sector, "Information Technology")
        self.assertEqual(holdings[0].weight, 7.10)

    def test_parser_applies_default_sector_for_spdr_rows(self) -> None:
        parser = HoldingsHtmlParser()
        holdings = parser.parse(
            SPDR_HTML,
            source_url="https://example.com/xlf",
            default_sector="XLF",
            etf_symbol="XLF",
        )

        self.assertEqual(len(holdings), 2)
        self.assertEqual(holdings[0].sector, "XLF")
        self.assertEqual(holdings[0].etf_symbol, "XLF")
        self.assertEqual(holdings[0].weight, 8.25)


class HoldingsExporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = make_test_dir("holdings_export")

    def tearDown(self) -> None:
        cleanup_test_dir(self.temp_dir)

    def test_exporter_writes_json_output(self) -> None:
        download_url = (
            "https://www.ishares.com/us/products/239726/"
            "ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
        )
        exporter = HoldingsExporter(
            session=FakeSession(
                {
                    download_url: ISHARES_CSV.encode("utf-8"),
                    "https://example.com/ivv": IVV_HTML,
                }
            )
        )
        output_path = exporter.export_ivv(self.temp_dir / "ivv.json", url="https://example.com/ivv")

        self.assertTrue(output_path.exists())
        payload = output_path.read_text(encoding="utf-8")
        self.assertIn("\"ticker\": \"AAPL\"", payload)
        self.assertIn("\"issuer_name\": \"Apple Inc.\"", payload)

    def test_parse_spdr_workbook(self) -> None:
        class FakeSheet:
            def iter_rows(self, values_only: bool = True):
                return iter(
                    [
                        ("Ticker", "Company Name", "Weight", "Sector"),
                        ("JPM", "JPMorgan Chase & Co.", 8.25, "Financials"),
                        ("BAC", "Bank of America Corp", 6.50, "Financials"),
                    ]
                )

        class FakeWorkbook:
            active = FakeSheet()

        exporter = HoldingsExporter(session=FakeSession({}))
        with patch("data_agg.holdings.load_workbook", return_value=FakeWorkbook()):
            holdings = exporter._parse_spdr_workbook(  # noqa: SLF001
                b"fake-workbook",
                source_url="https://example.com/xlf.xlsx",
                symbol="XLF",
            )

        self.assertEqual(len(holdings), 2)
        self.assertEqual(holdings[0].ticker, "JPM")
        self.assertEqual(holdings[0].sector, "Financials")


if __name__ == "__main__":
    unittest.main()
