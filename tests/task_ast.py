# tests/task_ast.py
"""How a `databricks/src` entry point is READ as an AST. No test lives here, and
that absence is the point -- pytest collects nothing from a module that matches no
`python_files` pattern.

THE THIRD FILE OF ANOTHER TWO-WAY SPLIT, and it exists for the reason
`tests/job_yaml.py` exists. `test_task_wiring.py` split twice before this on seams
where nothing was shared -- the YAML helpers went whole to `test_job_yaml_wiring.py`,
the capability sweep went whole to `test_serverless_capabilities.py`, and both of
those files' docstrings say so. The seam THIS split runs along, WHICH TABLE A TASK
TOUCHES versus WHICH MONTH IT USES, cuts straight through the readers instead: both
halves have to find a script's one `main()`, resolve a call inside it, and resolve a
local name it binds, before either can say anything about a table or a month.

SO THE READERS ARE EXTRACTED, NOT COPIED. Two copies of `sole_call` is two copies of
the assertion that makes a lock a lock -- "this lock is reading the wrong call, or
none, so it would pass on wiring it never saw" -- and the copy that goes stale is the
one whose failure message nobody has read in a year.

NOT IMPORTED FROM THE OTHER TEST MODULE, for `job_yaml.py`'s stated reason: a test
module importing another test module gives this suite a collection-order dependency
it does not otherwise have. A plain module under `tests/` has none.

WHAT IS HERE IS ONLY WHAT BOTH HALVES ASK. `kwarg` is NOT here -- only the month
half reads a keyword argument -- and neither are `spec_field`, `table_arg`, `deref`,
`resolved_spec`, `qualified_spec_fields` or `whole_spec_arg`, which resolve a
`opl.bronze.registry` spec and belong to the table half alone. A declaration in a
file that does not use it is a declaration nobody maintains.

THE `importlib` LOADER IS NOT HERE EITHER, and that is a different judgement rather
than an oversight: neither half of this split executes a job script. Eighteen test
modules load one by path, each with its own five-line `_load`, and hoisting that
idiom is a change to eighteen files rather than a consequence of this split."""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "databricks" / "src"


def main_of(script: str) -> ast.FunctionDef:
    tree = ast.parse((SRC / f"{script}.py").read_text(encoding="utf-8"), filename=f"{script}.py")
    mains = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"]
    assert len(mains) == 1, f"{script}.py does not define exactly one module-level main()"
    return mains[0]


def sole_call(main: ast.FunctionDef, name: str, script: str) -> ast.Call:
    """The one call to `name(...)` in the task's main, or a failure saying so.

    Modelled on `_gate_quarantine` in test_fail_on_dq_task.py, and for its reason:
    a lock that silently reads the first of several matches, or none at all,
    passes just as happily on wiring it never looked at."""
    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == name)
            or (isinstance(node.func, ast.Attribute) and node.func.attr == name)
        )
    ]
    assert len(calls) == 1, (
        f"{script}.py main() makes {len(calls)} call(s) to {name}(), expected exactly 1 -- "
        "this lock is reading the wrong call, or none, so it would pass on wiring it "
        "never saw"
    )
    return calls[0]


def locals_of(main: ast.FunctionDef, script: str) -> dict[str, ast.expr]:
    pairs = [
        (target.id, node.value)
        for node in ast.walk(main)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    ]
    names = [name for name, _ in pairs]
    assert len(names) == len(set(names)), (
        f"{script}.py main() binds a local name twice; this lock resolves a name to a "
        "single assignment and would pick an arbitrary one of them"
    )
    return dict(pairs)
