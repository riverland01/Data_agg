from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import requests
from bs4 import BeautifulSoup

from .utils import coerce_float


DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

SPDR_SECTOR_URLS = {
    "XLB": "https://www.ssga.com/us/en/intermediary/etfs/the-materials-select-sector-spdr-fund-xlb",
    "XLC": "https://www.ssga.com/us/en/intermediary/etfs/the-communication-services-select-sector-spdr-fund-xlc",
    "XLE": "https://www.ssga.com/us/en/intermediary/etfs/the-energy-select-sector-spdr-fund-xle",
    "XLF": "https://www.ssga.com/us/en/intermediary/etfs/the-financial-select-sector-spdr-fund-xlf",
    "XLI": "https://www.ssga.com/us/en/intermediary/etfs/the-industrial-select-sector-spdr-fund-xli",
    "XLK": "https://www.ssga.com/us/en/intermediary/etfs/the-technology-select-sector-spdr-fund-xlk",
    "XLP": "https://www.ssga.com/us/en/intermediary/etfs/the-consumer-staples-select-sector-spdr-fund-xlp",
    "XLRE": "https://www.ssga.com/us/en/intermediary/etfs/the-real-estate-select-sector-spdr-fund-xlre",
    "XLU": "https://www.ssga.com/us/en/intermediary/etfs/the-utilities-select-sector-spdr-fund-xlu",
    "XLV": "https://www.ssga.com/us/en/intermediary/etfs/the-health-care-select-sector-spdr-fund-xlv",
    "XLY": "https://www.ssga.com/us/en/intermediary/etfs/the-consumer-discretionary-select-sector-spdr-fund-xly",
}


class HoldingsExportError(RuntimeError):
    """Raised when a holdings source cannot be exported."""


@dataclass(slots=True)
class NormalizedHolding:
    ticker: str
    issuer_name: str
    sector: Optional[str]
    weight: Optional[float]
    cik: Optional[str] = None
    raw_identifier: Optional[str] = None
    source_url: Optional[str] = None
    etf_symbol: Optional[str] = None

    def as_record(self) -> dict:
        payload = {
            "ticker": self.ticker,
            "issuer_name": self.issuer_name,
            "sector": self.sector,
            "weight": self.weight,
            "cik": self.cik,
        }
        if self.raw_identifier:
            payload["raw_identifier"] = self.raw_identifier
        if self.source_url:
            payload["source_url"] = self.source_url
        if self.etf_symbol:
            payload["etf_symbol"] = self.etf_symbol
        return payload


class HoldingsHtmlParser:
    COLUMN_ALIASES = {
        "ticker": {"ticker", "symbol", "holding ticker", "fund ticker"},
        "issuer_name": {
            "issuer name",
            "company name",
            "name",
            "security",
            "holding name",
            "description",
        },
        "sector": {"sector", "gics sector", "market sector"},
        "weight": {"weight", "index weight", "portfolio weight", "% of net assets", "fund weight"},
        "cik": {"cik"},
    }

    def parse(self, html: str, source_url: str, default_sector: Optional[str] = None, etf_symbol: Optional[str] = None) -> list[NormalizedHolding]:
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[list[NormalizedHolding]] = []
        for table in soup.find_all("table"):
            parsed = self._parse_table(table, source_url=source_url, default_sector=default_sector, etf_symbol=etf_symbol)
            if parsed:
                candidates.append(parsed)
        if not candidates:
            raise HoldingsExportError(f"no parseable holdings table found for {source_url}")
        return max(candidates, key=len)

    def _parse_table(
        self,
        table,
        source_url: str,
        default_sector: Optional[str],
        etf_symbol: Optional[str],
    ) -> list[NormalizedHolding]:
        rows = table.find_all("tr")
        if len(rows) < 2:
            return []

        headers = self._extract_headers(rows[0])
        if not headers:
            return []
        mapping = self._map_headers(headers)
        if "ticker" not in mapping or "issuer_name" not in mapping:
            return []

        holdings: list[NormalizedHolding] = []
        for row in rows[1:]:
            values = self._extract_cells(row)
            if len(values) < len(headers):
                continue
            ticker = self._clean_text(values[mapping["ticker"]]).upper()
            issuer_name = self._clean_text(values[mapping["issuer_name"]])
            if not ticker or not issuer_name:
                continue
            sector = default_sector
            if "sector" in mapping:
                sector = self._clean_text(values[mapping["sector"]]) or default_sector
            cik = None
            if "cik" in mapping:
                cik = self._digits_only(values[mapping["cik"]]) or None
            weight = None
            if "weight" in mapping:
                weight = self._parse_weight(values[mapping["weight"]])

            holdings.append(
                NormalizedHolding(
                    ticker=ticker,
                    issuer_name=issuer_name,
                    sector=sector,
                    weight=weight,
                    cik=cik,
                    source_url=source_url,
                    etf_symbol=etf_symbol,
                )
            )
        return holdings

    def _extract_headers(self, row) -> list[str]:
        headers = [self._clean_text(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
        return [header for header in headers if header]

    def _extract_cells(self, row) -> list[str]:
        return [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]

    def _map_headers(self, headers: list[str]) -> dict[str, int]:
        mapped: dict[str, int] = {}
        normalized = [self._normalize_header(header) for header in headers]
        for canonical, aliases in self.COLUMN_ALIASES.items():
            for idx, header in enumerate(normalized):
                if header in aliases:
                    mapped[canonical] = idx
                    break
        return mapped

    def _normalize_header(self, value: str) -> str:
        return " ".join(value.strip().lower().replace("%", "").split())

    def _clean_text(self, value: str) -> str:
        return " ".join(value.strip().split())

    def _digits_only(self, value: str) -> str:
        return "".join(character for character in value if character.isdigit())

    def _parse_weight(self, value: str) -> Optional[float]:
        cleaned = value.replace("%", "").replace(",", "").strip()
        return coerce_float(cleaned)


class HoldingsExporter:
    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()
        self.parser = HoldingsHtmlParser()

    def fetch_html(self, url: str) -> str:
        response = self.session.get(url, headers=DEFAULT_HEADERS, timeout=30)
        response.raise_for_status()
        return response.text

    def export_ivv(self, output_path: str | Path, url: str) -> Path:
        html = self.fetch_html(url)
        holdings = self.parser.parse(html, source_url=url)
        return self._write(output_path, holdings)

    def export_spdr(self, output_path: str | Path, symbols: Optional[Iterable[str]] = None) -> Path:
        requested = list(symbols or SPDR_SECTOR_URLS.keys())
        holdings: list[NormalizedHolding] = []
        for symbol in requested:
            upper_symbol = symbol.upper()
            url = SPDR_SECTOR_URLS.get(upper_symbol)
            if not url:
                raise HoldingsExportError(f"unsupported SPDR sector ETF symbol: {symbol}")
            html = self.fetch_html(url)
            parsed = self.parser.parse(
                html,
                source_url=url,
                default_sector=upper_symbol,
                etf_symbol=upper_symbol,
            )
            holdings.extend(parsed)
        return self._write(output_path, holdings)

    def _write(self, output_path: str | Path, holdings: list[NormalizedHolding]) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [holding.as_record() for holding in holdings]
        path.write_text(json.dumps(rows, indent=2, ensure_ascii=True), encoding="utf-8")
        return path
