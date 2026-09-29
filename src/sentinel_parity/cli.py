# pattern: Imperative Shell
"""Typer command-line entry point."""

from __future__ import annotations

import os
import signal
from pathlib import Path  # noqa: TC003 - Typer evaluates option annotations at runtime.
from typing import Annotated

import typer

from sentinel_parity.io.config_loader import load_config
from sentinel_parity.runner import run

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    invoke_without_command=False,
    help="Compare SAS7BDAT outputs with Parquet outputs.",
)


@app.callback()
def root() -> None:
    """Compare SAS7BDAT outputs with Parquet outputs."""


@app.command("run")
def run_command(
    config: Annotated[
        Path | None, typer.Option("--config", help="TOML configuration file.")
    ] = None,
    sas_root: Annotated[Path | None, typer.Option("--sas-root", "-s")] = None,
    python_root: Annotated[Path | None, typer.Option("--python-root", "-p")] = None,
    output_dir: Annotated[Path | None, typer.Option("--output-dir", "-o")] = None,
    request_id: Annotated[
        str | None,
        typer.Option(
            "--id",
            help=(
                "Write the report into OUTPUT-DIR/<id>; letters, numbers, and underscores; "
                "at most 64 characters; details is reserved."
            ),
        ),
    ] = None,
    memory_limit: Annotated[str | None, typer.Option("--memory-limit")] = None,
    temp_dir: Annotated[Path | None, typer.Option("--temp-dir")] = None,
    max_temp_size: Annotated[str | None, typer.Option("--max-temp-size")] = None,
    preview_rows: Annotated[int | None, typer.Option("--preview-rows")] = None,
    preview_max_bytes: Annotated[int | None, typer.Option("--preview-max-bytes")] = None,
    preview_total_max_bytes: Annotated[
        int | None, typer.Option("--preview-total-max-bytes")
    ] = None,
    preview_cell_chars: Annotated[int | None, typer.Option("--preview-cell-chars")] = None,
    schema: Annotated[
        Path | None,
        typer.Option(
            "--schema",
            help=(
                "TOML file mapping table names to per-table pair_keys (join keys); section "
                "names are case-folded stems with one leading rNN_ prefix ignored."
            ),
        ),
    ] = None,
    pair_keys: Annotated[
        str | None,
        typer.Option(
            "--pair-keys",
            help=(
                "Comma-separated shared columns used as join keys: both tables are ordered "
                "by these columns and each row is compared only with the row whose key "
                "values are equal; rows with no key match are listed one-sided. Remaining "
                "shared columns order rows within equal keys."
            ),
        ),
    ] = None,
    no_pair_keys: Annotated[
        bool,
        typer.Option(
            "--no-pair-keys",
            help=(
                "Run without any declared pairing keys: overrides the TOML "
                "pair_keys, --pair-keys, and schema.toml for this run."
            ),
        ),
    ] = False,
    excel: Annotated[bool | None, typer.Option("--excel/--no-excel")] = None,
    excel_max_sheets: Annotated[int | None, typer.Option("--excel-max-sheets")] = None,
    excel_max_rows: Annotated[int | None, typer.Option("--excel-max-rows")] = None,
    excel_max_rows_per_sheet: Annotated[
        int | None, typer.Option("--excel-max-rows-per-sheet")
    ] = None,
    excel_max_bytes: Annotated[int | None, typer.Option("--excel-max-bytes")] = None,
    threads: Annotated[
        int | None,
        typer.Option("--threads", help="DuckDB worker threads for comparison. Default: 4."),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Echo structured run-log events to stderr as they happen."),
    ] = False,
    round: Annotated[
        int | None,
        typer.Option(
            "--round",
            "--round-digits",
            help="Round all numeric values to N digits after the decimal point before comparison.",
        ),
    ] = None,
) -> None:
    """Compare discovered datasets and write a local report."""
    declared_pair_keys: tuple[str, ...] | None = None
    if pair_keys and pair_keys.strip():
        names = tuple(name.strip() for name in pair_keys.split(",") if name.strip())
        declared_pair_keys = names or None
    if no_pair_keys:
        declared_pair_keys = ()
    try:
        config_value = load_config(
            config,
            {
                "sas_root": sas_root,
                "python_root": python_root,
                "output_dir": output_dir,
                "id": request_id,
                "memory_limit": memory_limit,
                "temp_dir": temp_dir,
                "max_temp_size": max_temp_size,
                "preview_rows": preview_rows,
                "preview_max_bytes": preview_max_bytes,
                "preview_total_max_bytes": preview_total_max_bytes,
                "preview_cell_chars": preview_cell_chars,
                "schema": schema,
                "pair_keys": declared_pair_keys,
                "excel": excel,
                "excel_max_sheets": excel_max_sheets,
                "excel_max_rows": excel_max_rows,
                "excel_max_rows_per_sheet": excel_max_rows_per_sheet,
                "excel_max_bytes": excel_max_bytes,
                "round_digits": round,
                "threads": threads,
            },
        )
        raise typer.Exit(run(config_value, verbose=verbose))
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(2) from exc
    except KeyboardInterrupt as exc:
        typer.echo("error: interrupted", err=True)
        raise typer.Exit(2) from exc
    except OSError as exc:
        detail = ""
        if exc.errno is not None:
            detail = f": [errno {exc.errno}] {exc.strerror or os.strerror(exc.errno)}"
        elif exc.strerror:
            detail = f": {exc.strerror}"
        typer.echo(f"error: operation failed ({type(exc).__name__}{detail})", err=True)
        raise typer.Exit(2) from exc
    except TypeError as exc:
        typer.echo(f"error: operation failed ({type(exc).__name__})", err=True)
        raise typer.Exit(2) from exc


def _interrupt_handler(signum: int, frame: object) -> None:
    raise KeyboardInterrupt


def main() -> None:
    signal.signal(signal.SIGTERM, _interrupt_handler)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, _interrupt_handler)
    app()


if __name__ == "__main__":
    main()
