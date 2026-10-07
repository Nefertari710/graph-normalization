#!/usr/bin/env python3
"""Plot hybrid-method cosine similarity distributions for the Address FD."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Sequence

import matplotlib
import numpy as np
from matplotlib.lines import Line2D

matplotlib.use("Agg")
import matplotlib.pyplot as plt


EMBEDDING_DIR = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = (
    EMBEDDING_DIR / "results" / "offshorecsv" / "embedding_offshore.json"
)
DEFAULT_OUTPUT = ANALYSIS_DIR / "cosine_similarity_ecdf.png"

CASE_NAME = "address_country"
METHODS = (
    ("fast_rp", "FastRP"),
    ("hash_gnn", "HashGNN"),
    ("graph_sage", "GraphSAGE"),
)
PAIR_SETS = (
    ("pairs", "Same determinant value", "#0072B2"),
    ("different_lhs_pairs", "Different determinant values", "#D55E00"),
)
REPRESENTATIONS = (
    ("denormalized", "De-normalized", "--"),
    ("normalized", "Normalized", "-"),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot per-seed ECDFs for FastRP, HashGNN, and GraphSAGE cosine "
            "similarities in the Address countries-to-country_codes case."
        )
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args(argv)


def load_runs(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    experiments = payload.get("experiments")
    if not isinstance(experiments, dict):
        raise ValueError(f"Missing object 'experiments' in {path}")

    runs = experiments.get(CASE_NAME)
    if not isinstance(runs, list) or not runs:
        raise ValueError(f"Missing experiment '{CASE_NAME}' in {path}")
    seeds = [int(run["random_seed"]) for run in runs]
    if len(seeds) != len(set(seeds)):
        raise ValueError(f"Duplicate random seeds in experiment '{CASE_NAME}'")
    return sorted(runs, key=lambda run: int(run["random_seed"]))


def extract_scores(
    run: dict[str, Any],
    pair_key: str,
    method_name: str,
    representation: str,
) -> np.ndarray:
    pairs = run.get(pair_key)
    if not isinstance(pairs, list) or not pairs:
        raise ValueError(
            f"Seed {run.get('random_seed')} has no pair data in '{pair_key}'"
        )

    scores = []
    for pair in pairs:
        try:
            score = float(
                pair["similarities"][method_name][representation]
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                f"Invalid {method_name} score for seed "
                f"{run.get('random_seed')} in '{pair_key}'"
            ) from error
        if not math.isfinite(score):
            raise ValueError(
                f"Non-finite score for seed {run.get('random_seed')}"
            )
        if score < -1.0 - 1e-9 or score > 1.0 + 1e-9:
            raise ValueError(
                f"Cosine similarity outside [-1, 1] for seed "
                f"{run.get('random_seed')}: {score}"
            )
        scores.append(min(1.0, max(-1.0, score)))

    return np.asarray(scores, dtype=float)


def ecdf_on_grid(scores: np.ndarray, grid: np.ndarray) -> np.ndarray:
    sorted_scores = np.sort(scores)
    return np.searchsorted(sorted_scores, grid, side="right") / len(
        sorted_scores
    )


def collect_scores(
    runs: list[dict[str, Any]],
) -> dict[tuple[str, str, str], list[np.ndarray]]:
    collected: dict[tuple[str, str, str], list[np.ndarray]] = {}
    for method_name, _ in METHODS:
        for representation, _, _ in REPRESENTATIONS:
            for pair_key, _, _ in PAIR_SETS:
                collected[(method_name, representation, pair_key)] = [
                    extract_scores(
                        run, pair_key, method_name, representation
                    )
                    for run in runs
                ]
    return collected


def plot_ecdfs(
    runs: list[dict[str, Any]], output: Path
) -> None:
    collected = collect_scores(runs)
    all_scores = np.concatenate(
        [scores for seed_scores in collected.values() for scores in seed_scores]
    )
    lower_bound = max(
        -1.0, math.floor((float(all_scores.min()) - 0.01) * 10) / 10
    )
    grid = np.linspace(lower_bound, 1.0, 1001)

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 16,
            "axes.titlesize": 18,
            "legend.fontsize": 8,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
        }
    )
    figure, axes = plt.subplots(
        1, 3, figsize=(7.2, 2.7), sharex=True, sharey=True
    )

    for axis, (method_name, method_label) in zip(axes, METHODS):
        for (
            representation,
            representation_label,
            line_style,
        ) in REPRESENTATIONS:
            for pair_key, pair_label, color in PAIR_SETS:
                seed_ecdfs = np.vstack(
                    [
                        ecdf_on_grid(scores, grid)
                        for scores in collected[
                            (method_name, representation, pair_key)
                        ]
                    ]
                )
                lower, median, upper = np.quantile(
                    seed_ecdfs, [0.25, 0.5, 0.75], axis=0
                )
                axis.fill_between(
                    grid,
                    lower,
                    upper,
                    color=color,
                    alpha=0.08,
                    linewidth=0,
                )
                axis.plot(
                    grid,
                    median,
                    color=color,
                    linestyle=line_style,
                    linewidth=1.8,
                    label=f"{pair_label}, {representation_label}",
                )

        axis.set_title(method_label)
        axis.set_xlabel("Cosine similarity")
        axis.set_xlim(lower_bound, 1.0)
        axis.set_ylim(0.0, 1.0)
        axis.grid(axis="y", color="#D0D0D0", linewidth=0.6, alpha=0.7)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)

    axes[0].set_ylabel("Cumulative fraction")
    legend_handles = [
        Line2D([0], [0], color="#0072B2", linewidth=1.8, label="Same"),
        Line2D([0], [0], color="#D55E00", linewidth=1.8, label="Different"),
        Line2D(
            [0],
            [0],
            color="#444444",
            linewidth=1.8,
            linestyle="-",
            label="Normalized",
        ),
        Line2D(
            [0],
            [0],
            color="#444444",
            linewidth=1.8,
            linestyle="--",
            label="De-normalized",
        ),
    ]
    axes[2].legend(
        handles=legend_handles,
        loc="upper left",
        ncol=2,
        frameon=False,
        columnspacing=0.8,
        handlelength=2.1,
        handletextpad=0.4,
        borderaxespad=0.4,
    )
    figure.subplots_adjust(
        top=0.86, bottom=0.19, left=0.08, right=0.99, wspace=0.12
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(
        output,
        dpi=600,
        bbox_inches="tight",
        facecolor="white",
        transparent=False,
    )
    plt.close(figure)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    runs = load_runs(args.input)
    plot_ecdfs(runs, args.output)
    print(f"Wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
