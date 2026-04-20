from __future__ import annotations

import argparse
import json
from pathlib import Path

from .charts import ChartService
from .config import load_registry
from .holdings import HoldingsExporter
from .models import ForwardPESnapshotRow
from .signals import SignalService
from .storage import create_store
from .universe import FailingAdapter, StaticRowsAdapter, UniverseService


def _load_fixture_rows(path: str | None) -> list[dict]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        return []
    return json.loads(file_path.read_text(encoding="utf-8"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="data-agg")
    parser.add_argument("--root", default="data/state", help="Storage root")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh = subparsers.add_parser("refresh-universes")
    refresh.add_argument("--date", required=True)
    refresh.add_argument("--dataset", default="ivv_holdings")
    refresh.add_argument("--fixture", help="Optional JSON fixture for manual recovery")

    diff = subparsers.add_parser("diff-universes")
    diff.add_argument("--date", required=True)
    diff.add_argument("--dataset", default="ivv_holdings")

    valuations = subparsers.add_parser("snapshot-valuations")
    valuations.add_argument("--date", required=True)
    valuations.add_argument("--fixture", help="Optional valuation JSON fixture")

    signals = subparsers.add_parser("compute-signals")
    signals.add_argument("--date", required=True)

    charts = subparsers.add_parser("render-charts")
    charts.add_argument("--date", required=True)
    charts.add_argument("--output-root", default="data/charts")

    export = subparsers.add_parser("export-universe-data")
    export.add_argument("--source", required=True, choices=["ivv", "spdr"])
    export.add_argument("--output", required=True)
    export.add_argument("--spdr-symbols", nargs="*", help="Optional subset of SPDR sector ETF symbols, e.g. XLK XLF XLV")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    store = create_store(args.root)
    registry = load_registry()

    if args.command == "refresh-universes":
        bundle = registry[args.dataset]
        fixture_rows = _load_fixture_rows(args.fixture)
        adapters = {
            "primary": FailingAdapter(bundle.primary) if not fixture_rows else StaticRowsAdapter(bundle.primary, fixture_rows),
            "fallback_1": FailingAdapter(bundle.fallback_1),
            "fallback_2": FailingAdapter(bundle.fallback_2),
            "manual_recovery": StaticRowsAdapter(bundle.manual_recovery, fixture_rows) if fixture_rows else FailingAdapter(bundle.manual_recovery),
        }
        rows = UniverseService(store).refresh_universe(bundle, args.date, adapters)
        print(json.dumps([row.as_record() for row in rows], indent=2))
        return 0

    if args.command == "diff-universes":
        changes = UniverseService(store).diff_universe(args.dataset, args.date)
        print(json.dumps([row.as_record() for row in changes], indent=2))
        return 0

    if args.command == "snapshot-valuations":
        rows = _load_fixture_rows(args.fixture)
        snapshots = [
            ForwardPESnapshotRow(
                as_of_date=args.date,
                member_id=row["member_id"],
                ticker=row["ticker"],
                provider_id=row.get("provider_id", "manual_fixture"),
                forward_eps=row.get("forward_eps"),
                forward_pe=row.get("forward_pe"),
                stale_days=int(row.get("stale_days", 0)),
                null_reason=row.get("null_reason"),
                methodology_tag=row.get("methodology_tag", "fixture"),
                source_url=row.get("source_url", ""),
                quality_flag=row.get("quality_flag", "accepted"),
            )
            for row in rows
        ]
        from .valuations import ValuationService

        ValuationService(store).snapshot_forward_pe(args.date, snapshots)
        print(json.dumps([row.as_record() for row in snapshots], indent=2))
        return 0

    if args.command == "compute-signals":
        signal = SignalService(store).compute_forward_pe_signal(args.date)
        print(json.dumps(signal.payload, indent=2))
        return 0

    if args.command == "render-charts":
        output_path = ChartService(store).render_forward_pe_chart_pack(args.date, root=args.output_root)
        print(str(output_path))
        return 0

    if args.command == "export-universe-data":
        exporter = HoldingsExporter()
        if args.source == "ivv":
            url = registry["ivv_holdings"].primary.url
            output_path = exporter.export_ivv(args.output, url=url)
        else:
            output_path = exporter.export_spdr(args.output, symbols=args.spdr_symbols)
        print(str(output_path))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
