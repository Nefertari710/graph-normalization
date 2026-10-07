#!/usr/bin/env python3
"""Compare normalized-update savings by table across TPC-H scale factors."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR.parent / "results"
DEFAULT_INPUTS = tuple(
    RESULTS_DIR / "new" / database / "new_redundancy_results.json"
    for database in ("tpch-sf-005", "tpch-sf-01", "tpch-sf-02")
)
DEFAULT_SPEEDUP_OUTPUT = (
    RESULTS_DIR / "sf_influence" / "update_speedup_by_strategy_region.png"
)

# The 16 REGION strategies, ordered by median copies updated at SF = 0.1.
STRATEGIES = (
    ("NR", 128),
    ("SNR", 192),
    ("CNR", 160),
    ("CNR + SNR", 224),
    ("PSSNR", 200),
    ("CNR + PSSNR", 232),
    ("OCNR", 176),
    ("OCNR + SNR", 240),
    ("OCNR + PSSNR", 248),
    ("LPSSNR", 201),
    ("LOCNR", 178),
    ("LOCNR + SNR", 242),
    ("CNR + LPSSNR", 233),
    ("LOCNR + PSSNR", 250),
    ("OCNR + LPSSNR", 249),
    ("LOCNR + LPSSNR", 251),
)

TARGETS = (
    ("R", "Region", STRATEGIES),
    ("N", "Nation", (
        ("SN", 64),
        ("CN", 32),
        ("CN + SN", 96),
        ("PSSN", 72),
        ("CN + PSSN", 104),
        ("OCN", 48),
        ("OCN + SN", 112),
        ("OCN + PSSN", 120),
        ("LPSSN", 73),
        ("LOCN", 50),
        ("LOCN + SN", 114),
        ("CN + LPSSN", 105),
        ("LOCN + PSSN", 122),
        ("OCN + LPSSN", 121),
        ("LOCN + LPSSN", 123),
    )),
    ("C", "Customer", (("OC", 16), ("LOC", 18))),
    ("S", "Supplier", (("PSS", 8), ("LPSS", 9))),
    ("P", "Part", (("PSP", 4), ("LPSP", 5))),
    ("PS", "PartSupp", (("LPS", 1),)),
    ("O", "Orders", (("LO", 2),)),
)


class AnalysisError(RuntimeError):
    pass


def samples_by_group(samples: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    indexed = {int(sample["group"]): sample for sample in samples}
    if len(indexed) != len(samples):
        raise AnalysisError("Duplicate update group in input JSON")
    return indexed


def same_update(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(left.get(field) == right.get(field)
               for field in ("group", "key", "a0", "a1"))


def scale_factor(database: str) -> float:
    prefix = "tpch-sf-"
    if not database.startswith(prefix):
        raise AnalysisError(f"Cannot infer scale factor from database {database!r}")
    token = database.removeprefix(prefix)
    if not token.isdigit():
        raise AnalysisError(f"Cannot infer scale factor from database {database!r}")
    return float(f"0.{token[1:]}") if token.startswith("0") else float(token)


def quartiles(values: list[float]) -> tuple[float, float]:
    cuts = statistics.quantiles(values, n=4, method="inclusive")
    return cuts[0], cuts[2]


def normalized_samples_for_config(
    report: dict[str, Any], config: dict[str, Any], terminal: str,
) -> dict[int, dict[str, Any]]:
    """Pair a configuration with the baseline measured in its own run."""
    source_run_id = config.get("source_run_id")
    if source_run_id is None:
        if "merge_provenance" in report:
            raise AnalysisError(
                f"Merged report mask {config['mask']} has no source_run_id"
            )
        source_report = report
    else:
        try:
            source_report = report["merge_provenance"]["source_runs"][
                source_run_id
            ]["header"]
        except (KeyError, TypeError) as exc:
            raise AnalysisError(
                f"Mask {config['mask']} has no baseline for source run "
                f"{source_run_id!r}"
            ) from exc
    return samples_by_group(
        source_report["normalized_baseline"]["updates"][terminal]["samples"]
    )


def analyse_report(
    path: Path,
    terminal: str,
    strategies: tuple[tuple[str, int], ...],
) -> dict[str, Any]:
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("experiment") != "tpch_256_fold_terminal_update":
        raise AnalysisError(f"Unexpected experiment in {path}")

    database = str(report["database"])
    target_masks = {mask for _, mask in strategies}
    configs = {
        int(config["mask"]): config
        for config in report["configurations"]
        if int(config["mask"]) in target_masks
    }
    missing = sorted(target_masks - set(configs))
    if missing:
        raise AnalysisError(f"{database} is missing masks {missing}")

    rows: list[dict[str, Any]] = []
    for name, mask in strategies:
        config = configs[mask]
        if config["terminal_relations"] != [terminal]:
            raise AnalysisError(
                f"{database} mask {mask} is not a {terminal}-only strategy"
            )
        baseline = normalized_samples_for_config(report, config, terminal)
        samples = samples_by_group(config["updates"][terminal]["samples"])
        if set(baseline) != set(samples):
            raise AnalysisError(
                f"{database} mask {mask} has mismatched {terminal} sample groups"
            )
        groups = sorted(samples)
        if not groups:
            raise AnalysisError(
                f"{database} mask {mask} has no paired {terminal} samples"
            )

        normalization_ms = float(config["normalization"]["elapsed_ms"])
        if normalization_ms < 0:
            raise AnalysisError(
                f"{database} mask {mask} has negative normalization time"
            )

        percentages: list[float] = []
        speedups: list[float] = []
        for group in groups:
            denormalized = samples[group]
            normalized = baseline[group]
            if not same_update(denormalized, normalized):
                raise AnalysisError(
                    f"{database} mask {mask}, group {group} is not paired"
                )
            denormalized_ms = float(denormalized["elapsed_ms"])
            normalized_ms = float(normalized["elapsed_ms"])
            if denormalized_ms <= 0 or normalized_ms <= 0:
                raise AnalysisError(
                    f"{database} mask {mask}, group {group} has non-positive time"
                )
            percentages.append(
                100.0 * (denormalized_ms - normalized_ms) / denormalized_ms
            )
            speedups.append(denormalized_ms / normalized_ms)

        pct_lower, pct_upper = quartiles(percentages)
        speedup_lower, speedup_upper = quartiles(speedups)
        rows.append(
            {
                "name": name,
                "mask": mask,
                "median_pct": statistics.median(percentages),
                "q1_pct": pct_lower,
                "q3_pct": pct_upper,
                "median_speedup": statistics.median(speedups),
                "q1_speedup": speedup_lower,
                "q3_speedup": speedup_upper,
                "normalization_s": normalization_ms / 1_000,
                "sample_count": len(percentages),
            }
        )

    return {
        "database": database,
        "sf": scale_factor(database),
        "repeat_count": int(report.get("update_repeat_count", 1)),
        "rows": rows,
    }


def draw_chart(
    datasets: list[dict[str, Any]],
    strategies: tuple[tuple[str, int], ...],
    table_name: str,
    output: Path,
    median_key: str,
    ylabel: str,
    reference: float,
    log_scale: bool = False,
) -> None:
    cache = Path(tempfile.gettempdir()) / "tpch_sf_percentage_cache"
    cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import LogFormatterSciNotation
    except ImportError as exc:
        raise AnalysisError("Matplotlib is required to draw the chart") from exc

    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 16,
            "axes.labelsize": 28,
            "xtick.labelsize": 16,
            "ytick.labelsize": 19,
            "legend.fontsize": 17,
        }
    )
    figure, (metric_axis, normalization_axis) = plt.subplots(
        1, 2, figsize=(14.0, 5.2), layout="constrained"
    )
    positions = list(range(len(strategies)))
    bar_width = 0.8 / len(datasets)
    colors = ("tab:green", "tab:blue", "tab:orange")

    for index, dataset in enumerate(datasets):
        medians = [row[median_key] for row in dataset["rows"]]
        offset = (index - (len(datasets) - 1) / 2) * bar_width
        bar_positions = [position + offset for position in positions]
        bars = metric_axis.bar(
            bar_positions,
            [median - reference for median in medians],
            bottom=reference,
            width=bar_width * 0.9,
            color=colors[index % len(colors)],
            label=f"SF = {dataset['sf']:g}",
        )
        normalization_axis.bar(
            bar_positions,
            [row["normalization_s"] for row in dataset["rows"]],
            width=bar_width * 0.9,
            color=bars.patches[0].get_facecolor(),
        )

    metric_axis.axhline(reference, color="#666666", linewidth=0.8)
    if log_scale:
        class CompactLogFormatter(LogFormatterSciNotation):
            """Omit the redundant multiplier for ticks in the 10^0 decade."""

            def __call__(self, value: float, pos: int | None = None) -> str:
                label = super().__call__(value, pos)
                return f"{value:g}" if label and 1 <= value < 10 else label

        metric_axis.set_yscale("log")
        metric_axis.yaxis.set_major_formatter(CompactLogFormatter())
        metric_axis.yaxis.set_minor_formatter(CompactLogFormatter())
    metric_axis.set_ylabel(ylabel)
    handles, legend_labels = metric_axis.get_legend_handles_labels()
    normalization_axis.legend(
        handles[::-1],
        legend_labels[::-1],
        frameon=False,
        loc="upper left",
    )
    normalization_axis.set_ylabel("Normalization time (s)")
    metric_axis.yaxis.label.set_y(0.46)
    normalization_axis.yaxis.label.set_y(0.46)

    labels = [str(index + 1) for index in positions]
    for axis in (metric_axis, normalization_axis):
        axis.set_xticks(positions, labels)
        axis.set_xlabel(f"Strategy index ({table_name})", fontsize=24)
        axis.tick_params(axis="x", labelsize=16)
        axis.tick_params(axis="y", labelsize=19)
        axis.grid(axis="y", color="#D8D8D8", linewidth=0.55)
        axis.set_axisbelow(True)
        for spine in axis.spines.values():
            spine.set_color("#777777")
            spine.set_linewidth(0.65)

    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(figure)


def render_markdown(
    datasets: list[dict[str, Any]],
    table_name: str,
    strategies: tuple[tuple[str, int], ...],
) -> str:
    repeat_counts = sorted({dataset["repeat_count"] for dataset in datasets})
    sample_counts = sorted(
        {row["sample_count"] for dataset in datasets for row in dataset["rows"]}
    )
    headers = [f"SF {dataset['sf']:g} Median (%)" for dataset in datasets]
    lines = [
        f"# {table_name} Update Percentage under Different TPC-H Scale Factors",
        "",
        (
            "`Normalization update saving (%) = 100 × "
            "(De-normalized − Normalized) / De-normalized`."
        ),
        "",
        "Positive values mean that the normalized update is faster.",
        "",
        (
            f"Each bar is the median of paired update percentages across "
            f"`{', '.join(map(str, sample_counts))}` cases. Each case time is "
            f"based on `{', '.join(map(str, repeat_counts))}` repetitions."
        ),
        "The right-hand panel shows each strategy's database normalization time.",
        "",
        "| Index | Edge set | " + " | ".join(headers) + " |",
        "|---|---|" + "---:|" * len(headers),
    ]
    for position, (name, _) in enumerate(strategies, start=1):
        values = [dataset["rows"][position - 1]["median_pct"]
                  for dataset in datasets]
        lines.append(
            "| " + " | ".join(
                (str(position), name, *(f"{value:+.2f}" for value in values))
            ) + " |"
        )
    return "\n".join(lines) + "\n"


def render_speedup_markdown(
    datasets: list[dict[str, Any]],
    table_name: str,
    strategies: tuple[tuple[str, int], ...],
) -> str:
    repeat_counts = sorted({dataset["repeat_count"] for dataset in datasets})
    sample_counts = sorted(
        {row["sample_count"] for dataset in datasets for row in dataset["rows"]}
    )
    headers = [f"SF {dataset['sf']:g} Median (×)" for dataset in datasets]
    lines = [
        f"# {table_name} Update Speedup under Different TPC-H Scale Factors",
        "",
        "`Normalization speedup = De-normalized / Normalized`.",
        "",
        "Values above `1×` mean that the normalized update is faster.",
        "",
        (
            f"Each bar is the median of paired speedups across "
            f"`{', '.join(map(str, sample_counts))}` cases. Each case time is "
            f"based on `{', '.join(map(str, repeat_counts))}` repetitions."
        ),
        "The right-hand panel shows each strategy's database normalization time.",
        "",
        "| Index | Edge set | " + " | ".join(headers) + " |",
        "|---|---|" + "---:|" * len(headers),
    ]
    for position, (name, _) in enumerate(strategies, start=1):
        values = [dataset["rows"][position - 1]["median_speedup"]
                  for dataset in datasets]
        lines.append(
            "| " + " | ".join(
                (str(position), name, *(f"{value:.2f}" for value in values))
            ) + " |"
        )
    return "\n".join(lines) + "\n"


def output_for_target(base: Path, terminal: str, table_name: str) -> Path:
    if terminal == "R":
        return base
    base_stem = base.stem.removesuffix("_region")
    return base.with_name(
        f"{base_stem}_{table_name.lower()}{base.suffix}"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot normalized-update metrics across TPC-H scale factors"
    )
    parser.add_argument(
        "--input",
        action="append",
        type=Path,
        help=(
            "result JSON; repeat for multiple SFs. By default, results for "
            "SF 0.05, 0.1, and 0.2 under results/new are used"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="optional update-saving percentage chart output",
    )
    parser.add_argument(
        "--speedup-output",
        type=Path,
        default=DEFAULT_SPEEDUP_OUTPUT,
        help=(
            "REGION speedup chart output; other table names are appended "
            "before the file extension"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    inputs = args.input or DEFAULT_INPUTS

    percentage_output: Path | None = (
        args.output.expanduser().resolve() if args.output is not None else None
    )
    speedup_output = args.speedup_output.expanduser().resolve()
    written: list[Path] = []
    try:
        datasets_by_target: dict[str, list[dict[str, Any]]] = {}
        for terminal, table_name, strategies in TARGETS:
            datasets = sorted(
                (
                    analyse_report(
                        path.expanduser().resolve(), terminal, strategies
                    )
                    for path in inputs
                ),
                key=lambda dataset: dataset["sf"],
            )
            scale_factors = [dataset["sf"] for dataset in datasets]
            if len(scale_factors) != len(set(scale_factors)):
                raise AnalysisError("Duplicate scale factor in input reports")
            datasets_by_target[terminal] = datasets

            table_output = output_for_target(
                speedup_output, terminal, table_name
            )
            table_markdown_output = table_output.with_suffix(".md")
            draw_chart(
                datasets,
                strategies,
                table_name,
                table_output,
                "median_speedup",
                "Update speedup (×)",
                1,
                log_scale=True,
            )
            table_markdown_output.write_text(
                render_speedup_markdown(datasets, table_name, strategies),
                encoding="utf-8",
            )
            written.extend((table_markdown_output, table_output))

        if percentage_output is not None:
            percentage_markdown_output = percentage_output.with_suffix(".md")
            draw_chart(
                datasets_by_target["R"],
                STRATEGIES,
                "Region",
                percentage_output,
                "median_pct",
                "Update saving (%)",
                0,
            )
            percentage_markdown_output.write_text(
                render_markdown(
                    datasets_by_target["R"], "Region", STRATEGIES
                ),
                encoding="utf-8",
            )
            written[0:0] = (percentage_markdown_output, percentage_output)
    except (AnalysisError, KeyError, TypeError, ValueError,
            json.JSONDecodeError, OSError) as exc:
        print(f"SF influence analysis failed: {exc}", file=sys.stderr)
        return 1

    for path in written:
        print(f"Wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
