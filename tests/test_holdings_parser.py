from __future__ import annotations

import unittest

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


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, mapping: dict[str, str]) -> None:
        self.mapping = mapping

    def get(self, url: str, headers=None, timeout: int = 30):  # noqa: ANN001
        if url not in self.mapping:
            raise AssertionError(f"unexpected URL: {url}")
        return FakeResponse(self.mapping[url])


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
        exporter = HoldingsExporter(session=FakeSession({"https://example.com/ivv": IVV_HTML}))
        output_path = exporter.export_ivv(self.temp_dir / "ivv.json", url="https://example.com/ivv")

        self.assertTrue(output_path.exists())
        payload = output_path.read_text(encoding="utf-8")
        self.assertIn("\"ticker\": \"AAPL\"", payload)


if __name__ == "__main__":
    unittest.main()
