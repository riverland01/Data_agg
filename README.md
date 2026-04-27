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
- `export-universe-data --source ivv|spdr --output PATH`

## Getting Started

Start in PowerShell from the repo root:

```powershell
cd <path-to-repo>
$env:PYTHONPATH = (Join-Path (Get-Location) 'src')
$python = '<path-to-python.exe>'
```

Examples:

- `<path-to-repo>` might be `D:\Repos\Data_agg`
- `<path-to-python.exe>` might be a system Python, a virtualenv Python, or a Conda environment Python

If you are already using the right Python environment in your terminal, you can skip the `$python = ...` variable and replace `& $python` with `python`.

### Day 1 Workflow

If this is your first run, use this sequence:

```powershell
& $python -m data_agg export-universe-data --source ivv --output data\inputs\ivv_2026-04-20.json
& $python -m data_agg refresh-universes --date 2026-04-20 --fixture data\inputs\ivv_2026-04-20.json
& $python -m data_agg diff-universes --date 2026-04-20
```

What each step does:

- `export-universe-data`: fetches public holdings data and writes normalized JSON.
- `refresh-universes`: loads that JSON into the pipeline as the accepted universe snapshot for the date.
- `diff-universes`: compares the date's snapshot to the previous one.

On your very first run, there is no earlier baseline yet, so the diff output is mostly a sanity check.

### Full Example Run

Once you have a universe file and valuation file, a fuller workflow looks like this:

```powershell
cd <path-to-repo>
$env:PYTHONPATH = (Join-Path (Get-Location) 'src')
$python = '<path-to-python.exe>'

& $python -m data_agg export-universe-data --source ivv --output data\inputs\ivv_2026-04-20.json
& $python -m data_agg refresh-universes --date 2026-04-20 --fixture data\inputs\ivv_2026-04-20.json
& $python -m data_agg diff-universes --date 2026-04-20
& $python -m data_agg snapshot-valuations --date 2026-04-20 --fixture tests\fixtures\forward_pe_2026-04-20.json
& $python -m data_agg compute-signals --date 2026-04-20
& $python -m data_agg render-charts --date 2026-04-20
```

This sequence means:

- create today's IVV-based universe JSON,
- load it into storage,
- inspect membership changes,
- load valuation data,
- compute the aggregate forward P/E signal,
- write chart-ready output.

### Second Day And Later

Tomorrow, run the same export and refresh steps again with the new date and output file:

```powershell
& $python -m data_agg export-universe-data --source ivv --output data\inputs\ivv_2026-04-21.json
& $python -m data_agg refresh-universes --date 2026-04-21 --fixture data\inputs\ivv_2026-04-21.json
& $python -m data_agg diff-universes --date 2026-04-21
```

That is when the point-in-time universe tracking becomes useful, because the pipeline can compare today's membership to yesterday's accepted snapshot.

## Export Universe JSON

You can create ready-to-use universe JSON files directly from the public holdings pages.

Examples:

```powershell
$env:PYTHONPATH = (Join-Path (Get-Location) 'src')
$python = '<path-to-python.exe>'

& $python -m data_agg export-universe-data --source ivv --output data\inputs\ivv_2026-04-20.json
& $python -m data_agg export-universe-data --source spdr --output data\inputs\spdr_2026-04-20.json
& $python -m data_agg export-universe-data --source spdr --spdr-symbols XLK XLF XLV --output data\inputs\spdr_subset_2026-04-20.json
```

The output JSON is in the same shape expected by `refresh-universes`, so you can pass it directly into the next step.

## Files Written To Disk

Most commands either write a user-facing JSON file under `data\inputs` / `data\charts`, or update pipeline state under `data\state`.

### State Directory

By default, persistent pipeline state is written under:

```text
data\state
```

If `duckdb` is not available, these are JSONL files. Each line is one stored record.

Common files in `data\state`:

- `universe_snapshot.jsonl`
  Purpose: the dated history of accepted universe memberships. This is the main point-in-time universe table.
- `universe_change_log.jsonl`
  Purpose: detected changes between one accepted universe snapshot and the prior one, such as adds, removes, ticker changes, and sector reclassifications.
- `security_master.jsonl`
  Purpose: identifier and lineage history for securities, including ticker, issuer name, sector, and active/inactive status over time.
- `forward_pe_snapshot.jsonl`
  Purpose: stored company-level forward P/E and forward EPS snapshots for a given date.
- `source_run_log.jsonl`
  Purpose: operational log of source runs, including which provider was used, whether it succeeded, and whether a fallback or stale snapshot was used.
- `chart_manifest.jsonl`
  Purpose: metadata about chart-ready outputs, including date, provenance label, health, and output location.

### Command By Command Output

#### `export-universe-data`

Example:

```powershell
& $python -m data_agg export-universe-data --source ivv --output data\inputs\ivv_2026-04-27.json
```

Writes:

- the file you specify with `--output`

Typical examples:

- `data\inputs\ivv_2026-04-27.json`
- `data\inputs\spdr_2026-04-27.json`

Purpose:

- this is a normalized input file you can inspect manually
- it is designed to be passed directly into `refresh-universes`
- it is not the pipeline's historical store; it is an import file

#### `refresh-universes`

Example:

```powershell
& $python -m data_agg refresh-universes --date 2026-04-27 --fixture data\inputs\ivv_2026-04-27.json
```

Writes or updates:

- `data\state\universe_snapshot.jsonl`
- `data\state\universe_change_log.jsonl`
- `data\state\security_master.jsonl`
- `data\state\source_run_log.jsonl`

Purpose:

- stores the accepted universe membership for the date
- records changes relative to the previous accepted snapshot
- updates security identifier history
- logs whether the source was accepted, stale, or failed over

#### `diff-universes`

Example:

```powershell
& $python -m data_agg diff-universes --date 2026-04-27
```

Writes:

- normally nothing new beyond printing the diff result to the terminal

Reads from:

- `data\state\universe_snapshot.jsonl`

Purpose:

- inspect how one stored snapshot differs from the previous accepted snapshot

#### `snapshot-valuations`

Example:

```powershell
& $python -m data_agg snapshot-valuations --date 2026-04-27 --fixture path\to\forward_pe_2026-04-27.json
```

Writes or updates:

- `data\state\forward_pe_snapshot.jsonl`

Purpose:

- stores company-level forward P/E / forward EPS values for that date
- provides the valuation coverage needed for `compute-signals`

#### `compute-signals`

Example:

```powershell
& $python -m data_agg compute-signals --date 2026-04-27
```

Writes:

- no new user-facing file by itself

Reads from:

- `data\state\universe_snapshot.jsonl`
- `data\state\forward_pe_snapshot.jsonl`

Purpose:

- calculates the aggregated forward P/E output for the broad universe and sectors
- prints the computed JSON to the terminal

If valuation coverage is missing or partial, some or all `forward_pe` fields can be `null`. That is expected.

#### `render-charts`

Example:

```powershell
& $python -m data_agg render-charts --date 2026-04-27
```

Writes or updates:

- chart payload file under `data\charts`
- `data\state\chart_manifest.jsonl`

Purpose:

- writes chart-ready JSON output that another visualization layer can use
- records metadata about that output in the chart manifest

The current scaffold writes JSON chart payloads, not PNG images.

## Notes

- The code is written to be resilient to missing local dependencies. If `duckdb` is not installed, it falls back to JSONL storage.
- Free sources such as Yahoo Finance are isolated behind provider boundaries so they can break without bringing down the full pipeline.
- Universe history is append-only. Past memberships are never overwritten by current memberships.
