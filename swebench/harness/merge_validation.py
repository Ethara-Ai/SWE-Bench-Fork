"""Merge validation reports into dataset JSONL.

Reads gold/empty report.json files produced by run_validation.py,
computes FAIL_TO_PASS and PASS_TO_PASS for each instance, and writes
an enriched JSONL file compatible with SWE-bench evaluation format.

Usage:
    python -m swebench.harness.merge_validation \
        --dataset_name /path/to/raw.jsonl \
        --run_id sqlfluff_val2 \
        --output /path/to/enriched.jsonl

If --output is omitted, writes to <dataset_name>.validated.jsonl
"""

from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from argparse import ArgumentParser
from pathlib import Path

KEY_INSTANCE_ID = "instance_id"
DEFAULT_LOGS_DIR = Path("logs/run_evaluation")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_report(
    logs_dir: Path, run_id: str, pred_type: str, instance_id: str
) -> dict | None:
    report_path = logs_dir / run_id / pred_type / instance_id / "report.json"
    if not report_path.exists():
        return None
    with open(report_path) as f:
        data = json.load(f)
    return data.get(instance_id)


def compute_f2p_p2p(
    gold_report: dict, empty_report: dict
) -> tuple[list[str], list[str]]:
    gold_pass = set(gold_report["tests_status"]["PASS"])
    empty_pass = set(empty_report["tests_status"]["PASS"])
    empty_fail = set(empty_report["tests_status"]["FAIL"])

    fail_to_pass = sorted(gold_pass & empty_fail)
    pass_to_pass = sorted(gold_pass & empty_pass)
    return fail_to_pass, pass_to_pass


def main(
    dataset_name: str,
    run_id: str,
    output: str | None,
    log_dir: str | None,
    min_f2p: int,
    discard_empty_f2p: bool,
):
    dataset_path = Path(dataset_name).resolve()
    if not dataset_path.exists():
        logger.error(f"Dataset not found: {dataset_path}")
        sys.exit(1)

    logs_dir = Path(log_dir) if log_dir else DEFAULT_LOGS_DIR

    with open(dataset_path) as f:
        instances = [json.loads(line) for line in f if line.strip()]

    logger.info(f"Loaded {len(instances)} instances from {dataset_path}")

    enriched = []
    skipped_missing = []
    skipped_f2p = []
    skipped_gold_fail = []

    for instance in instances:
        iid = instance[KEY_INSTANCE_ID]

        gold_report = load_report(logs_dir, run_id, "gold", iid)
        empty_report = load_report(logs_dir, run_id, "empty", iid)

        if gold_report is None or empty_report is None:
            missing = []
            if gold_report is None:
                missing.append("gold")
            if empty_report is None:
                missing.append("empty")
            logger.warning(f"[{iid}] Missing reports: {', '.join(missing)} — skipping")
            skipped_missing.append(iid)
            continue

        if not gold_report.get("patch_successfully_applied"):
            logger.warning(f"[{iid}] Gold patch failed to apply — skipping")
            skipped_gold_fail.append(iid)
            continue

        gold_fails = gold_report["tests_status"]["FAIL"]
        if gold_fails:
            logger.warning(
                f"[{iid}] Gold patch has {len(gold_fails)} test failures — skipping"
            )
            skipped_gold_fail.append(iid)
            continue

        f2p, p2p = compute_f2p_p2p(gold_report, empty_report)

        if len(f2p) < min_f2p:
            logger.warning(
                f"[{iid}] Only {len(f2p)} FAIL_TO_PASS tests (min={min_f2p}) — "
                f"{'discarding' if discard_empty_f2p else 'keeping'}"
            )
            if discard_empty_f2p:
                skipped_f2p.append(iid)
                continue

        instance["FAIL_TO_PASS"] = json.dumps(f2p)
        instance["PASS_TO_PASS"] = json.dumps(p2p)
        enriched.append(instance)
        logger.info(f"[{iid}] F2P={len(f2p)}, P2P={len(p2p)}")

    if output is None:
        output = str(dataset_path.with_suffix("")) + ".validated.jsonl"

    output_path = Path(output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=output_path.parent, suffix=".tmp", prefix=".merge_"
    )
    try:
        with os.fdopen(tmp_fd, "w") as f:
            for instance in enriched:
                f.write(json.dumps(instance) + "\n")
        os.replace(tmp_path, output_path)
    except BaseException:
        os.unlink(tmp_path)
        raise

    logger.info(f"Wrote {len(enriched)} enriched instances to {output_path}")
    if skipped_missing:
        logger.info(f"Skipped {len(skipped_missing)} (missing reports): {skipped_missing}")
    if skipped_gold_fail:
        logger.info(f"Skipped {len(skipped_gold_fail)} (gold failures): {skipped_gold_fail}")
    if skipped_f2p:
        logger.info(f"Discarded {len(skipped_f2p)} (empty F2P): {skipped_f2p}")

    if not enriched:
        logger.error("No instances survived — check reports and filters")
        sys.exit(1)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset_name", required=True, help="Path to input JSONL dataset"
    )
    parser.add_argument(
        "--run_id", required=True, help="Validation run ID (matches run_validation --run_id)"
    )
    parser.add_argument(
        "--output", default=None, help="Output JSONL path (default: <input>.validated.jsonl)"
    )
    parser.add_argument(
        "--log_dir", default=None,
        help="Root log directory (default: logs/run_evaluation)",
    )
    parser.add_argument(
        "--min_f2p", type=int, default=1,
        help="Minimum FAIL_TO_PASS tests required per instance (default: 1)",
    )
    parser.add_argument(
        "--discard_empty_f2p", action="store_true",
        help="Discard instances with fewer than --min_f2p FAIL_TO_PASS tests",
    )
    args = parser.parse_args()
    main(
        dataset_name=args.dataset_name,
        run_id=args.run_id,
        output=args.output,
        log_dir=args.log_dir,
        min_f2p=args.min_f2p,
        discard_empty_f2p=args.discard_empty_f2p,
    )
