from __future__ import annotations

import argparse
import json
from pathlib import Path

from alt_hot_scanner.data.binance_public import write_json_exclusive
from alt_hot_scanner.universe.adjudications import load_lifecycle_adjudications
from alt_hot_scanner.universe.evidence_replay import run_full_evidence_replay


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Approval-time full replay of primitive lifecycle evidence"
    )
    parser.add_argument("--report-dir", required=True)
    parser.add_argument("--adjudications", required=True)
    parser.add_argument(
        "--output", default="full_evidence_verification_report.json"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    report_root = Path(args.report_dir).resolve(strict=True)
    inventory = json.loads((report_root / "candidate_inventory.json").read_text("utf-8"))
    adjudications = load_lifecycle_adjudications(
        Path(args.adjudications),
        candidate_set_digest=inventory["candidate_set_digest"],
    )
    result = run_full_evidence_replay(
        report_root,
        repository_root=root,
        lifecycle_adjudications=adjudications,
    )
    output = report_root / args.output
    if output.exists():
        existing = json.loads(output.read_text(encoding="utf-8"))
        if existing != result:
            raise FileExistsError("Existing full replay report differs")
    else:
        write_json_exclusive(output, result)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
