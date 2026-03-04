"""CLI for YAML-driven walk-forward experiment grids."""

from __future__ import annotations

import argparse

from src.research.experiment_runner import run_from_config


def main() -> int:
    parser = argparse.ArgumentParser(description="Run YAML-defined walk-forward research experiments")
    parser.add_argument("--config", required=True, help="Path to YAML config with parameter_grid")
    parser.add_argument("--output-dir", default="data/experiments", help="Directory for persisted run artifacts")
    args = parser.parse_args()

    paths = run_from_config(config_path=args.config, output_dir=args.output_dir)

    print("Experiment grid completed")
    print(f"Run directory: {paths['run_dir']}")
    print(f"Summary report: {paths['summary_report']}")
    print(f"Artifacts manifest: {paths['artifact_manifest']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
