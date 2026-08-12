# AGENTS.md

## Project Overview

This repository is owned by the ObsInt Processing team. For a service description and local setup, start with `README.md`.

## Team context

Load and follow the **team-info** skill for Shared Standards, PR rules, Go/Python conventions, testing norms, related services, and deployment flow:

- Skill source: https://github.com/RedHatInsights/processing-tools/blob/master/skills/team-info/SKILL.md
- Install (example): `npx skills add RedHatInsights/processing-tools --skill team-info -g -a cursor -y` (Cursor) or `… -a claude-code -y` (Claude Code)

## Working with this Repository

**As an agent, you should create a TODO list** when working on tasks to track progress and ensure all steps are completed systematically.

Prefer commands from `README.md`, `Makefile`, or `CONTRIBUTING.md` when those files exist in the repo.

Before opening a PR, use the repo’s pre-push checks when present (`make before_commit` / `make before-commit`, or `pre-commit run --all-files`) and follow team-info Pull Request Requirements and Testing.

ClowdApp / deploy manifests often live under `deploy/` when this repo is a deployed service; stage/prod promotion is described in team-info Deployment Flow.

## External References

Use whichever of these exist for this repository:

- `README.md` (repo root)
- [team-info skill](https://github.com/RedHatInsights/processing-tools/blob/master/skills/team-info/SKILL.md)
- [Quality](https://ccx.pages.redhat.com/ccx-docs/docs/processing/quality/) — testing strategy and expectations
- [ProdSec](https://ccx.pages.redhat.com/ccx-docs/docs/processing/prodsec/) — branch protection and merge controls
- [insights-behavioral-spec](https://github.com/RedHatInsights/insights-behavioral-spec) — BDD for many services (when this repo has scenarios there)
