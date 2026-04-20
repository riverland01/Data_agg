# Data_agg

Resilient public-source financial data pipeline scaffolding focused on:

- source breakage as a normal operational case,
- point-in-time universe history,
- ETF and index membership updates,
- forward P/E snapshots that survive free-source volatility,
- explicit provenance and staleness labeling.

## What is implemented

- A Python package under `src/data_agg`.
- A source registry with source bundles and fallback slots in `config/sources.json`.
- Storage backends:
  - `DuckDBParquetStore` when `duckdb` is available.
  - `JsonArtifactStore` fallback when it is not.
- Core data models for:
  - `universe_snapshot`
  - `universe_change_log`
  - `security_master`
  - `forward_pe_snapshot`
  - `source_run_log`
- A resilient universe refresh service that:
  - validates snapshots,
  - diffs against the latest accepted snapshot,
  - records normal reconstitutions as change events,
  - carries forward stale snapshots when policy allows.
- Valuation aggregation utilities for sector and broad forward P/E.
- Chart manifest rendering with explicit universe basis and data quality labels.
- Tests covering:
  - universe diffs,
  - stale fallback behavior,
  - point-in-time replay.

## CLI

The package exposes these commands:

- `refresh-universes --date YYYY-MM-DD`
- `diff-universes --date YYYY-MM-DD`
- `snapshot-valuations --date YYYY-MM-DD`
- `compute-signals --date YYYY-MM-DD`
- `render-charts --date YYYY-MM-DD`

## Notes

- The code is written to be resilient to missing local dependencies. If `duckdb` is not installed, it falls back to JSONL storage.
- Free sources such as Yahoo Finance are isolated behind provider boundaries so they can break without bringing down the full pipeline.
- Universe history is append-only. Past memberships are never overwritten by current memberships.
