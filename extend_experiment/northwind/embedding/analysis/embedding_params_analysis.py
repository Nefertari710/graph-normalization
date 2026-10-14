"""Analyze independent property/structure and maximum-hop sweeps."""

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

# Keep Matplotlib and Fontconfig caches out of the experiment directory.
CACHE_DIR = Path(tempfile.gettempdir()) / "embedding_params_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(CACHE_DIR / "matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(CACHE_DIR))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DEFAULT_INPUT = Path(__file__).resolve().parents[1] / "embedding_results_undirection.json"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent
METHODS = ("fast_rp", "hash_gnn")
METHOD_NAMES = {"fast_rp": "FastRP", "hash_gnn": "HashGNN"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze both independent sweeps in embedding_results_undirection.json."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--experiment", choices=("both", "parameters", "hops"), default="both"
    )
    return parser.parse_args()


def load_parameter_rows(
        path: Path, sweep_key: str
) -> tuple[list[dict[str, Any]], list[str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    experiments = data["experiments"]
    rows: list[dict[str, Any]] = []
    case_order = list(experiments)

    for case_name, case_result in experiments.items():
        sweeps = case_result[sweep_key]
        for method in METHODS:
            sweep = sweeps[method]
            parameter = sweep["parameter"]
            for point in sweep["points"]:
                similarities = point["similarities"]
                normalized = similarities["normalized"]
                denormalized = similarities["denormalized"]
                delta = similarities["delta_normalized_minus_denormalized"]
                rows.append(
                    {
                        "case": case_name,
                        "method": method,
                        "parameter": parameter,
                        "value": float(point["value"]),
                        "normalized_mean": float(normalized["mean"]),
                        "normalized_median": float(normalized["median"]),
                        "denormalized_mean": float(denormalized["mean"]),
                        "denormalized_median": float(denormalized["median"]),
                        "delta_mean": float(delta["mean"]),
                        "delta_median": float(delta["median"]),
                        "absolute_delta_mean": abs(float(delta["mean"])),
                        "absolute_delta_median": abs(float(delta["median"])),
                    }
                )

    return rows, case_order


def make_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((row["case"], row["method"]), []).append(row)

    for (case_name, method), group in groups.items():
        closest = min(group, key=lambda item: item["absolute_delta_median"])
        largest = max(group, key=lambda item: item["absolute_delta_median"])
        summary.append(
            {
                "case": case_name,
                "method": method,
                "parameter": closest["parameter"],
                "closest_value": closest["value"],
                "closest_normalized_median": closest["normalized_median"],
                "closest_denormalized_median": closest["denormalized_median"],
                "closest_delta_median": closest["delta_median"],
                "closest_absolute_delta_median": closest["absolute_delta_median"],
                "largest_gap_value": largest["value"],
                "largest_gap_normalized_median": largest["normalized_median"],
                "largest_gap_denormalized_median": largest["denormalized_median"],
                "largest_gap_delta_median": largest["delta_median"],
                "largest_gap_absolute_delta_median": largest[
                    "absolute_delta_median"
                ],
            }
        )

    return summary


def method_case_rows(
        rows: list[dict[str, Any]], method: str, case_name: str
) -> list[dict[str, Any]]:
    return sorted(
        (
            row
            for row in rows
            if row["method"] == method and row["case"] == case_name
        ),
        key=lambda row: row["value"],
    )


def finish_figure(fig: plt.Figure, output_path: Path) -> None:
    fig.tight_layout(rect=(0.02, 0.02, 1, 0.91))
    fig.savefig(output_path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def add_figure_header(fig: plt.Figure, title: str, legend_columns: int) -> None:
    handles, labels = fig.axes[0].get_legend_handles_labels()
    fig.suptitle(title, y=0.92, fontsize=15, fontweight="bold")
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.9),
        ncol=legend_columns,
        frameon=False,
    )


def plot_median_summary(
        rows: list[dict[str, Any]], case_order: list[str], method: str,
        output_path: Path, sweep_title: str
) -> None:
    fig, axes = plt.subplots(3, 3, figsize=(15, 11), sharey=True)
    for index, (axis, case_name) in enumerate(zip(axes.flat, case_order)):
        case_rows = method_case_rows(rows, method, case_name)
        values = [row["value"] for row in case_rows]
        for field, label, color, marker, style in (
                ("normalized_median", "Normalized median", "#2563eb", "o", "-"),
                ("denormalized_median", "Denormalized median", "#ea580c", "s", "-"),
                ("delta_median", "Delta median", "#7c3aed", "^", "--"),
        ):
            axis.plot(
                values,
                [row[field] for row in case_rows],
                color=color,
                marker=marker,
                linestyle=style,
                linewidth=1.8,
                markersize=4,
                label=label,
            )
        axis.axhline(0, color="#374151", linewidth=0.9)
        axis.set_title(case_name.replace("_", " "), fontsize=10)
        axis.set_xticks(values)
        axis.grid(alpha=0.25)
        if index // 3 == 2:
            axis.set_xlabel(case_rows[0]["parameter"])
        if index % 3 == 0:
            axis.set_ylabel("Median similarity / delta")

    add_figure_header(
        fig,
        f"{METHOD_NAMES[method]} {sweep_title} summary (Delta = Norm - Denorm)",
        3,
    )
    finish_figure(fig, output_path)


def print_summary(summary: list[dict[str, Any]]) -> None:
    headers = [
        "case",
        "method",
        "closest value",
        "closest |delta|",
        "largest value",
        "largest |delta|",
    ]
    table_rows = [
        [
            row["case"],
            METHOD_NAMES[row["method"]],
            f'{row["closest_value"]:g}',
            f'{row["closest_absolute_delta_median"]:.6f}',
            f'{row["largest_gap_value"]:g}',
            f'{row["largest_gap_absolute_delta_median"]:.6f}',
        ]
        for row in summary
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in table_rows))
        for index in range(len(headers))
    ]

    def format_row(row: list[str]) -> str:
        return " | ".join(
            value.ljust(widths[index]) for index, value in enumerate(row)
        )

    print(format_row(headers))
    print("-+-".join("-" * width for width in widths))
    for row in table_rows:
        print(format_row(row))


def main() -> None:
    args = parse_args()
    experiments = (
        ("parameters", "parameter_sweeps", "embedding_params_results", "parameter sweep", ""),
        ("hops", "hop_sweeps", "embedding_hops_results", "maximum hops", "_hops"),
    )
    for name, sweep_key, directory, title, suffix in experiments:
        if args.experiment not in ("both", name):
            continue
        output_dir = args.output_dir / directory
        output_dir.mkdir(parents=True, exist_ok=True)
        rows, case_order = load_parameter_rows(args.input, sweep_key)
        summary = make_summary(rows)
        for method in METHODS:
            plot_median_summary(rows, case_order, method, output_dir / f"summary_{method}{suffix}.png", title)
        print(f"Loaded {len(case_order)} cases and {len(rows)} {title} points.\n")
        print_summary(summary)
        print(f"\nResults written to: {output_dir}\n")


if __name__ == "__main__":
    main()
