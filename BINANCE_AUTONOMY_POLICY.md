# Binance Research Autonomy Policy V1

## Purpose

Run only pre-authorized, bounded research work without requiring an interactive chat session, while preserving the repository's frozen research specification and leakage controls.

## Boundary

This runtime is research-only. It does not authorize live trading, order placement, wallets/private keys, leverage, position sizing, copy trading, or live-capital deployment.

## Authority

Git history plus frozen repository artifacts are authoritative. Runtime state is operational evidence only. A task becomes AUTO-runnable only through a committed JSON manifest under `tasks/queued/` with `auto_authorized=true`.

## Allowed autonomous actions

The scheduler may claim and execute an approved task manifest; invoke only the repository Python interpreter against a script under `scripts/` or `python -m pytest/ruff`; record declared reads, writes, mutable resources, and provider dependencies for audit; retry failures only up to the manifest's bounded retry limit; recover interrupted tasks from durable state at daemon startup; and report runtime status.

It must not silently widen a command, provider, schema, time horizon, data split, or research question. A changed manifest under an existing task ID becomes `DECISION_REQUIRED` rather than silently replacing the old authorization. Domain-specific scripts remain responsible for enforcing their own output and data-integrity boundaries.

## Status semantics

`QUEUED`, `RUNNING`, `WAITING_FOR_PRIMARY`, `WAITING_EXTERNAL`, `BLOCKED`, `DECISION_REQUIRED`, `COMPLETED`, `FAILED`, and `CANCELLED` are the only task states.

## AUTO qualification

AUTO is earned only when all current checks pass:

1. `binance-research-scheduler.service` is active.
2. The lifecycle canary has completed and an idempotent replay has been observed.
3. Bounded retry is active with a hard cap of three attempts.
4. No queued manifest has drifted under an existing task ID.
5. No task is in exhausted `FAILED` state.
6. At least one real bounded task with `task_kind=research` has completed through the scheduler.
7. A scheduler service restart has been observed after a completed research task, with no duplicate attempt of that completed task.
8. No task is left `RUNNING` at the qualification checkpoint.

AUTO is revoked whenever a required current gate stops passing. Historical qualification never overrides a current failed gate.

## Initial concurrency policy

V1 intentionally uses one serial primary lane. Binance full-history acquisition and lifecycle artifacts have strict integrity and authorization boundaries, so introducing concurrent writers before local runtime evidence would add risk without research value. A second lane requires a later version with explicit artifact, state, provider, and Git-worktree isolation tests.
