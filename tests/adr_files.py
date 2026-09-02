# tests/adr_files.py
"""Where the ADRs live, how the two index scripts are loaded, and how a phase is compared
to a branch. No test lives here, and that absence is the point.

THE THIRD FILE OF A TWO-WAY SPLIT, the shape `tests/job_yaml.py` already has in this
suite. `tests/test_adr_index.py` reached 796 lines against a strictly-under-800 cap, and
the next thing to be added to it was the handling for an ADR written in a phase that has
not merged yet -- so the file was split first, along the seam its own docstring already
named: WHAT THE PAGE SAYS AGAINST WHAT THE FILES SAY, which stays there, versus WHAT IS
DECLARED IN `scripts/adr_index.py` AGAINST WHAT GIT SAYS, which is now
`tests/test_adr_phase_declaration.py`.

THAT SEAM CUTS THROUGH THE READERS, so they are extracted rather than copied. Both halves
locate the ADR directory, both load `scripts/adr_index.py` by path, and the declaration
half needs the phase-token comparison that the CI-runnable label check also uses. A copy
would be a second spelling of `docs/adr` in the one test file whose entire subject is what
a duplicated list forgets.

NOT IMPORTED FROM THE OTHER TEST MODULE, for the reason `tests/job_yaml.py` gives: a test
module importing another test module gives the suite a collection-order dependency it does
not otherwise have. A plain module under `tests/` has none -- pytest collects nothing from
it, because it matches no `python_files` pattern, and it declares no fixture.

WHAT IS NOT HERE: the MASK. `test_adr_index.py` duplicates `adr_index._mask_non_prose` on
purpose, so that it goes on reading the ADR files when `scripts/adr_index.py` refuses to
import, and `test_the_two_maskings_agree_over_the_battery` is what holds the two together.
Moving it here would collapse that duplication into the import it was written to avoid.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ADR_DIR = REPO / "docs" / "adr"
INDEX = ADR_DIR / "README.md"


def load(name: str, filename: str):
    """One script under `scripts/`, loaded by path and REGISTERED under `name` before it
    executes.

    Registered first for two reasons that both bite silently: `dataclasses` resolves
    `KW_ONLY` through `sys.modules[cls.__module__]` and raises on a module that is not
    there yet, and `generate_adr_index.py` does `import adr_index`, which would otherwise
    load a SECOND copy whose declarations could drift from the one these tests assert on.

    CALLED INSIDE THE TESTS THAT NEED IT, NEVER AT MODULE SCOPE, and that is not style.
    `scripts/adr_index.py` REFUSES AT IMPORT when an ADR has no declared phase. At module
    scope that refusal becomes a collection error and takes the file-reading tests down
    with it, so the run reports *"could not import"* where it should report *"0021 has no
    row in the index"* -- and it is why the mask is duplicated rather than imported."""
    spec = importlib.util.spec_from_file_location(name, REPO / "scripts" / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def adr_paths() -> list[Path]:
    """Every ADR file, in number order. `README.md` is not matched by the glob."""
    return sorted(ADR_DIR.glob("0*.md"))


def phase_tokens(text: str) -> list[str]:
    """A phase label or a branch name as its lowercased alphanumeric runs."""
    return re.findall(r"[a-z0-9]+", text.lower())


def branch_names_phase(phase: str, branch: str) -> bool:
    """Whether `branch`'s tokens carry `phase`'s, contiguously and in order.

    TOKENS RATHER THAN A FLATTENED STRING, because flattening cannot separate a phase from
    its own sub-phase and this table holds exactly that pair. The first spelling stripped
    both to `[a-z0-9]` and asked for substring containment, so `F1.4` -> `f14` was found
    inside `feat/f1-4b-pr-b-second-month` and `F1.4b` -> `f14b` was found inside
    `feat/f1-4-bronze-generalisation` -- the `b` coming from `bronze`. It rejected `F9`,
    which is a phase from another universe, and accepted the only confusion available
    here. Split on the separators, `f1`, `4` and `4b` stay distinct tokens and both
    swaps fail."""
    want, have = phase_tokens(phase), phase_tokens(branch)
    return any(have[at:at + len(want)] == want for at in range(len(have) - len(want) + 1))
