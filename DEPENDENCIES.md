# Dependency maintenance

Prepared 2026-11-01 on branch `chore/dependency-guardrails`.

Both next-major boundaries are now strict. Existing gldb and ontolutils pins remain unchanged. Existing unrelated source edits and staged work are outside this change.

| File | Before | After |
| --- | --- | --- |
| pyproject.toml | `h5rdmtoolbox>=2.7.3,<=3.0.0` | `h5rdmtoolbox>=2.7.3,<3.0.0` |
| pyproject.toml | `ssnolib>=2.2.0.3,<=3.0.0.0` | `ssnolib>=2.2.0.3,<3.0.0.0` |

Dependencies on related packages must exclude the next major release.
Retain tighter existing pins unless an upgrade is deliberate and verified.
Pre-1.0 API changes can also require a minor-version ceiling.
Self-references that compose extras are exempt.

For future work: inspect status, create a task branch before editing, preserve
existing changes, stage explicit paths, inspect the staged diff, run appropriate
checks, and commit on that branch. Keep this record, README, and duplicated
requirements in sync. Ask for explicit approval before starting subagents.

The workspace coordination repository contains the complete inventory and
repeatable checks in `docs/dependency-audit.md` and
`tools/audit_dependencies.py`. The audit covers manifests and emitted package
metadata; version ranges alone are not a runtime compatibility guarantee.

## Verification

Workspace manifest audit: 14 folders, 39 manifests, 40 cross-package
requirements checked; all related dependencies exclude their next major.
All h5rdmtoolbox requirements accept 2.8.3 and reject 3.0.0.

Setuptools emitted package metadata successfully in a temporary metadata-only
scaffold using the actual manifests. Related Requires-Dist bounds passed.
This verifies dependency metadata rather than building or installing the package.

Broad application suites and a full online dependency resolution were not run.
These checks do not certify every release in the permitted version ranges.
