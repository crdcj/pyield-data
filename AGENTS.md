# AGENTS

## Mission
Reduce project complexity by default.

## General principle
Apply Occam's razor to all code work: among solutions that satisfy the current
requirements, prefer the one with the fewest assumptions, branches, dependencies,
and abstractions. Keep complexity that protects correctness or addresses a concrete
failure mode. Do not simplify at the expense of required behavior.

## Rules
- Prefer the simplest change that solves the current problem.
- Add complexity only when strictly necessary and justified by a concrete failure mode.
- Avoid speculative abstractions and premature generalization.
- Keep code paths small, explicit, and easy to debug.
- Before introducing non-trivial logic, state why it is needed.
- Favor local fixes over broad refactors unless requested.

## Verification
- Before concluding any change, run `uv run ruff check .`.
- Run `uv run ruff format --check .`.
- Run `uv run ty check`.
