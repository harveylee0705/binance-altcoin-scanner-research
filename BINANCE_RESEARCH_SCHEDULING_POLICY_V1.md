# Binance Research Scheduling Policy V1

Status: `IMPLEMENTED IN REPO — RUNTIME QUALIFICATION REQUIRED`

## Objective

Provide a deterministic AUTO execution layer for the existing frozen Binance Scanner v0.1 research program without changing the research question, data split rules, lifecycle evidence rules, or acquisition authorization gates.

## Scheduling

V1 is serial. Among `QUEUED` committed task manifests, lower priority number runs first: P0, P1, P2, P3, P4. Ties break by `task_id`. A completed task is never re-executed under the same manifest hash. An interrupted task returns to `QUEUED` only while bounded attempts remain.

## Manifest contract

Every AUTO task must declare: `task_id`, `program_id`, `priority`, `command`, `timeout_seconds`, `max_attempts`, `writes`, `reads`, `mutable_resources`, `provider_dependencies`, `auto_authorized`, and `task_kind`.

The scheduler binds durable state to the exact SHA-256 of the manifest bytes. Editing an existing task manifest does not silently change authorization. It moves the task to `DECISION_REQUIRED`.

## Research integrity

The scheduler does not bypass existing lifecycle approvals, plan validation, split guards, checksums, frozen cutoffs, or future-data restrictions. Existing scripts remain responsible for their domain-specific gates. AUTO only decides when an already-authorized bounded command runs.

## Runtime state

Canonical runtime state is `.auto_runtime/research_scheduler_v1.json`. The directory is local-only and must not be committed. The scheduler writes JSON atomically.

## Qualification sequence

1. Sync the Work PC checkout to the commit containing this runtime.
2. Run focused scheduler tests and the existing repository test suite.
3. Run the lifecycle canary twice and verify idempotent replay.
4. Commit one real bounded research task manifest that is safe under the frozen research spec.
5. Install/start the user service.
6. Let the service claim and complete that task once.
7. Restart the service and verify no duplicate execution.
8. Run `qualify-auto`. Only a full pass permits the project state to say AUTO.
