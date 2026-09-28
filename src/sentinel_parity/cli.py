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
