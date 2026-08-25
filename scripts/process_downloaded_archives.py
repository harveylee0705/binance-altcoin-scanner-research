from __future__ import annotations

import argparse
from pathlib import Path

from alt_hot_scanner.data.acquisition import verify_frozen_plan
from alt_hot_scanner.data.full_history import (
    build_4h_stage,
    build_lifecycle_stage,
    build_normalized_1h_stage,
    build_scanner_engineering_stage,
    derive_gate_a,
    derive_gate_b,
    derive_gate_c,
    derive_gate_d,
    derive_gate_e,
    write_gate,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Canonical performance-blind build: Gate A -> normalized 1H -> B -> 4H -> C -> lifecycle -> D -> sealed Scanner engineering -> E"
    )
    parser.add_argument("--plan", required=True)
    parser.add_argument("--raw-completion", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    verified = verify_frozen_plan(root / args.plan, root)
    run_id = verified["plan"]["run_identity"]["run_id"]
    raw_root = root / "data" / "raw"
    completion = (root / args.raw_completion).resolve()
    run_root = root / "data" / "canonical" / run_id
    gate_root = run_root / "gates"

    gate_a_path = gate_root / "gate_A.json"
    write_gate(gate_a_path, derive_gate_a(verified, completion, raw_root))

    normalized = run_root / "normalized_1h"
    build_normalized_1h_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        target=normalized,
    )
    gate_b_path = gate_root / "gate_B.json"
    write_gate(
        gate_b_path,
        derive_gate_b(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            normalized_stage=normalized,
        ),
    )

    bars_4h = run_root / "completed_4h"
    build_4h_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        target=bars_4h,
    )
    gate_c_path = gate_root / "gate_C.json"
    write_gate(
        gate_c_path,
        derive_gate_c(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            stage_4h=bars_4h,
        ),
    )

    lifecycle = run_root / "lifecycle_eligibility"
    build_lifecycle_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        gate_c_path=gate_c_path,
        stage_4h=bars_4h,
        target=lifecycle,
    )
    gate_d_path = gate_root / "gate_D.json"
    write_gate(
        gate_d_path,
        derive_gate_d(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            gate_c_path=gate_c_path,
            stage_4h=bars_4h,
            lifecycle_stage=lifecycle,
        ),
    )

    scanner = run_root / "scanner_engineering_sealed"
    build_scanner_engineering_stage(
        verified_plan=verified,
        completion_path=completion,
        raw_root=raw_root,
        gate_a_path=gate_a_path,
        gate_b_path=gate_b_path,
        normalized_stage=normalized,
        gate_c_path=gate_c_path,
        stage_4h=bars_4h,
        gate_d_path=gate_d_path,
        lifecycle_stage=lifecycle,
        target=scanner,
    )
    gate_e_path = gate_root / "gate_E.json"
    write_gate(
        gate_e_path,
        derive_gate_e(
            verified_plan=verified,
            completion_path=completion,
            raw_root=raw_root,
            gate_a_path=gate_a_path,
            gate_b_path=gate_b_path,
            normalized_stage=normalized,
            gate_c_path=gate_c_path,
            stage_4h=bars_4h,
            gate_d_path=gate_d_path,
            lifecycle_stage=lifecycle,
            scanner_stage=scanner,
        ),
    )
    print(f"Canonical performance-blind build complete through Gate E: {run_root}")
    print("No Gate F was created. No Scanner performance aggregate was emitted.")


if __name__ == "__main__":
    main()
