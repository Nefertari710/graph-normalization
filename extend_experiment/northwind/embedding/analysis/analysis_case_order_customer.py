"""Compare Natural and Undirected FastRP sweeps for ``order_customer``."""

import argparse
from pathlib import Path

try:
    from . import embedding_params_analysis as analysis
except ImportError:
    import embedding_params_analysis as analysis


CASE_NAME = "order_customer"
METHOD = "fast_rp"
EMBEDDING_DIR = Path(__file__).resolve().parents[1]
NATURAL_RESULT_FILE_NAME = "embedding_results_natrual.json"
UNDIRECTED_RESULT_FILE_NAME = "embedding_results_undirection.json"
DEFAULT_NATURAL_INPUT = EMBEDDING_DIR / NATURAL_RESULT_FILE_NAME
DEFAULT_UNDIRECTED_INPUT = EMBEDDING_DIR / UNDIRECTED_RESULT_FILE_NAME
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "case"
OUTPUT_BASENAME = "embedding_case"
FIGURE_SIZE = (14, 9)
AXIS_LABEL_FONT_SIZE = 26
Y_AXIS_LABEL_FONT_SIZE = 28
TICK_LABEL_FONT_SIZE = 22
LEGEND_FONT_SIZE = 20
PANEL_TITLE_FONT_SIZE = 18
LINE_WIDTH = 4.5
MARKER_SIZE = 11
MARKER_EDGE_WIDTH = 1.1
OUTPUT_DPI = 300


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Combine the Natural and Undirected FastRP sweeps for order_customer "
            "in one figure."
        )
    )
    parser.add_argument(
        "--natural-input", type=Path, default=DEFAULT_NATURAL_INPUT
    )
    parser.add_argument(
        "--undirected-input", type=Path, default=DEFAULT_UNDIRECTED_INPUT
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--output-name",
        "--result-name",
        dest="output_name",
        default=OUTPUT_BASENAME,
        help="Output filename for the combined figure, with or without .png.",
    )
    return parser.parse_args()


def normalize_result_name(name: str) -> str:
    result_name = name.removesuffix(".png")
    if not result_name or Path(result_name).name != result_name:
        raise ValueError("--output-name must be a non-empty filename, not a path.")
    return result_name


def plot_case_panel(
        axis: object,
        rows: list[dict[str, object]],
        panel_title: str,
        show_x_axis: bool,
        show_y_axis: bool,
) -> None:
    case_rows = analysis.method_case_rows(rows, METHOD, CASE_NAME)
    if not case_rows:
        raise ValueError(
            f"No {METHOD} data found for case {CASE_NAME!r} in the input file."
        )

    values = [row["value"] for row in case_rows]
    for field, label, color, marker, style in (
            ("normalized_median", "Normalized median", "#2563eb", "o", "-"),
            ("denormalized_median", "Denormalized median", "#ea580c", "s", "-"),
            (
                "delta_median",
                "Delta median (Norm - Denorm)",
                "#7c3aed",
                "^",
                "--",
            ),
    ):
        axis.plot(
            values,
            [row[field] for row in case_rows],
            color=color,
            marker=marker,
            linestyle=style,
            linewidth=LINE_WIDTH,
            markersize=MARKER_SIZE,
            markeredgecolor="white",
            markeredgewidth=MARKER_EDGE_WIDTH,
            solid_capstyle="round",
            zorder=3,
            label=label,
        )

    axis.axhline(0, color="#374151", linewidth=0.9)
    axis.text(
        0.02,
        0.96,
        panel_title,
        transform=axis.transAxes,
        horizontalalignment="left",
        verticalalignment="top",
        fontsize=PANEL_TITLE_FONT_SIZE,
        fontweight="bold",
    )
    axis.set_xticks(values)
    axis.tick_params(axis="both", labelsize=TICK_LABEL_FONT_SIZE)
    if show_x_axis:
        axis.set_xlabel(
            str(case_rows[0]["parameter"]), fontsize=AXIS_LABEL_FONT_SIZE
        )
    else:
        axis.tick_params(axis="x", bottom=False, labelbottom=False)
    if not show_y_axis:
        axis.tick_params(axis="y", left=False, labelleft=False)
    axis.grid(alpha=0.25)


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result_name = normalize_result_name(args.output_name)

    orientations = (
        ("Natural", args.natural_input),
        ("Undirected", args.undirected_input),
    )
    sweeps = (
        ("parameter_sweeps", "property ratio"),
        ("hop_sweeps", "hops"),
    )

    fig, axes = analysis.plt.subplots(
        2, 2, figsize=FIGURE_SIZE, sharex="col", sharey=True
    )
    for row_index, (orientation, input_path) in enumerate(orientations):
        for column_index, (sweep_key, sweep_title) in enumerate(sweeps):
            rows, _ = analysis.load_parameter_rows(input_path, sweep_key)
            plot_case_panel(
                axes[row_index, column_index],
                rows,
                f"{orientation}: {sweep_title}",
                show_x_axis=row_index == len(orientations) - 1,
                show_y_axis=column_index == 0,
            )

    handles, labels = axes[0, 0].get_legend_handles_labels()
    axes[0, 0].legend(
        handles,
        labels,
        loc="upper right",
        bbox_to_anchor=(0.98, 0.86),
        frameon=False,
        fontsize=LEGEND_FONT_SIZE,
        markerscale=1.1,
    )
    fig.supylabel(
        "Median similarity / delta", fontsize=Y_AXIS_LABEL_FONT_SIZE, x=0.04
    )
    fig.tight_layout(rect=(0.02, 0.03, 1, 0.98), h_pad=1.5, w_pad=1.5)

    output_path = args.output_dir / f"{result_name}.png"
    fig.savefig(output_path, dpi=OUTPUT_DPI, bbox_inches="tight")
    analysis.plt.close(fig)
    print(f"Wrote: {output_path}")


if __name__ == "__main__":
    main()
