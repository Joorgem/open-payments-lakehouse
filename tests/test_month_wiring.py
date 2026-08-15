"""Which MONTH each job task uses, and that every consumer inside one task uses the
same one.

SPLIT OUT OF `tests/test_task_wiring.py` BY F-DB TASK 1, at exactly 783 of this
project's 800-line cap, with F-DB's Postgres ingest entry point still to be added to
both halves. That file's own title is "Which table each job task touches", and this
is the one lock in it that was never about a table: the month is a JOB PARAMETER, it
appears in no registry, and `_MONTH_CONSUMERS` below names call sites -- a landing
directory, an Auto Loader checkpoint, an audit column -- rather than staging, bronze
or quarantine. That file changes when a table's COORDINATES change; this one changes
when an entry point gains or loses a consumer of the window it was launched for.

WHAT THE TWO HALVES SAY TOGETHER, because neither is a wiring claim alone. The table
half pins that one spec, resolved from argv, feeds every coordinate -- so a task
cannot read one table's landing dir and write another's staging. This half pins that
one month, bound by `require_month`, feeds every consumer -- so a task cannot read
2026-07's files under 2026-06's checkpoint and stamp them with a third value. A task
can satisfy either one completely while failing the other, which is why each file's
docstring points at the other.

THE READERS WENT TO `tests/job_yaml.py`'s SIBLING, `tests/task_ast.py`, AND WERE NOT
COPIED. This seam, unlike the previous two out of `test_task_wiring.py`, cuts through
the AST helpers: both halves resolve a script's one `main()`, one call inside it, and
the locals it binds. `_kwarg` below stayed here because only this half reads a keyword
argument -- `tests/task_ast.py`'s docstring states that rule and this is it applied.

Nothing here starts Spark, and nothing here executes a job script: every assertion is
read off the AST, so it sees the wiring rather than a run."""
from __future__ import annotations

import ast

import pytest
from task_ast import locals_of, main_of, sole_call


def _kwarg(call: ast.Call, name: str, where: str) -> ast.expr:
    """The value of a `name=` keyword on `call`, or a failure saying it is absent.

    An absent keyword must be a red test rather than a skipped assertion: the whole
    point below is that the month is passed EVERYWHERE it is consumed, so "not
    passed" is the defect, not a case to tolerate."""
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    raise AssertionError(f"{where}: this call has no {name}= keyword")


# Every place an ingest entry point hands the month to something, as
# (callee, positional index, keyword) -- exactly one of the last two is used.
# ENUMERATED, not discovered: a fifth consumer added without an entry here would
# be a fifth chance for the month to diverge, and a lock that globbed for `month`
# would pass on whatever it happened to find.
_MONTH_CONSUMERS: dict[str, list[tuple[str, int | None, str | None]]] = {
    "bronze_ingest": [
        # the directory the files are READ from
        ("landing_table", 1, None),
        # the state that records which of those files have been read
        ("bronze_stream", None, "month"),
        ("checkpoint_location", None, "month"),
        # the value stamped into every row
        ("add_audit_columns", None, "snapshot_month"),
    ],
    "bronze_lookup_ingest": [
        ("bronze_lookup_stream", 2, None),
        ("checkpoint_location", None, "month"),
        ("add_audit_columns", None, "snapshot_month"),
    ],
    # F1b Task 3. Four consumers again, and the same four FACTS -- the directory read,
    # the state that records which of its files have been read, and the value stamped
    # into every row -- reached through the second landing root and the four-column
    # audit stamp a generated source takes.
    "bronze_payments_ingest": [
        ("landing_generated_table", 1, None),
        ("bronze_stream", None, "month"),
        ("checkpoint_location", None, "month"),
        ("add_common_audit_columns", None, "snapshot_month"),
    ],
    # The WRITER, and it belongs in this lock for the same reason every reader does:
    # its month decides which directory the stream is written into, and the ingest task
    # that follows resolves ITS source dir from the same job parameter. A month that
    # diverged here would write one month's landing dir and read another's -- a job
    # whose every task reports SUCCESS having ingested nothing. The staging dir is the
    # second consumer because a file staged under one month and replaced into another
    # is a cross-device rename that fails, or worse, does not.
    "generate_payments": [
        ("landing_generated_table", 1, None),
        ("landing_generated_tmp", 1, None),
    ],
    # F-API Task 2. Four consumers again, and the same four FACTS -- reached through the
    # THIRD landing root and through `registry_landing.landing_dir`, which resolves the
    # root from the spec's landing mode rather than from this entry point knowing the
    # layout. The month is the third positional argument there, not the second, because
    # that function takes the whole spec.
    "bronze_ptax_ingest": [
        ("landing_dir", 2, None),
        ("bronze_stream", None, "month"),
        ("checkpoint_location", None, "month"),
        ("add_common_audit_columns", None, "snapshot_month"),
    ],
    # THE FETCHER, and it belongs in this lock for `generate_payments`' reason: its month
    # decides which directory the record is written into, and the ingest task that
    # follows resolves ITS source dir from the same job parameter. A month that diverged
    # here would write one month's landing dir and read another's -- a job whose every
    # task reports SUCCESS having ingested nothing, after 42 HTTP round trips.
    #
    # Through `registry_landing`'s pair rather than `landing_api_table`/`landing_api_tmp`
    # since F-API's fix pass, so the month is the THIRD positional argument here: this
    # task built the api root's path itself while `bronze_ptax_ingest` asked `landing_dir`
    # for it, which is one directory resolved two ways inside one job.
    "fetch_ptax": [
        ("landing_dir", 2, None),
        ("landing_tmp_dir", 2, None),
    ],
}


@pytest.mark.parametrize("script", sorted(_MONTH_CONSUMERS))
def test_every_consumer_of_the_month_reads_the_one_required_local(script):
    """ONE month local, bound by `require_month`, feeding every consumer.

    `bronze_ingest.py` already said why for two of them -- "the SAME `month` the
    stream read from -- one local, fed to both, so the snapshot the rows are
    stamped with cannot drift from the folder they came out of". F1.4b PR B Task 5
    Step 0 added a third and fourth consumer, and the third is the one that makes
    this a lock rather than a tidiness check: the Auto Loader checkpoint. Keyed on a
    month that is not the source directory's, a run drains 2026-07's files under
    2026-06's checkpoint -- Spark's recovery semantics call a changed source
    "generally not allowed ... likely to fail with unpredictable errors".

    The second half is that the local is `require_month`'s result. A local bound to
    `DEFAULT.month`, or to `args[2] if len(args) > 2 else DEFAULT.month`, would
    satisfy every consistency check above while being the ONE value four entry
    points have already substituted by accident (`require_month`'s own docstring
    names them). Consistent and wrong is what this repo keeps paying for.

    Reads the AST, so it sees the wiring rather than a run: nothing imports Spark."""
    main = main_of(script)
    scope = locals_of(main, script)
    names: set[str] = set()
    for callee, position, keyword in _MONTH_CONSUMERS[script]:
        where = f"{script}: {callee}"
        call = sole_call(main, callee, script)
        passed = call.args[position] if keyword is None else _kwarg(call, keyword, where)
        assert isinstance(passed, ast.Name), (
            f"{where} is handed {ast.dump(passed)[:80]}, not a local name -- a second "
            "lookup of the month here can name a month the rest of this run did not use"
        )
        names.add(passed.id)
    assert len(names) == 1, (
        f"{script}.py feeds the month to its consumers from {sorted(names)} -- more than "
        "one local, so the checkpoint, the source dir and the stamped column can diverge"
    )
    bound = scope.get(names.pop())
    assert (
        isinstance(bound, ast.Call)
        and isinstance(bound.func, ast.Name)
        and bound.func.id == "require_month"
    ), (
        f"{script}.py's month local is not `require_month(...)`'s result; a defaulted "
        "month is consistent across every consumer above and still wrong in all of them"
    )
    # AND THE SAME PROPERTY FROM THE OTHER SIDE, because the enumeration above cannot
    # see a consumer it does not name: a fifth one added tomorrow as
    # `something(..., month=DEFAULT.month)` satisfies every assertion so far by being
    # invisible to them. This needs no list -- the substitution has exactly one
    # spelling, an attribute access for `month` on the config -- so it catches the
    # consumer this file has not been taught about yet. `require_month`'s docstring
    # names four entry points that each wrote it; two of them are these.
    #
    # THE SECOND DOOR: the owner is matched on its LAST dotted component, not on being
    # a bare `Name`. `isinstance(node.value, ast.Name)` saw `DEFAULT.month` and missed
    # `opl.config.DEFAULT.month` -- the same substitution written with the import spelled
    # out, which is a normal thing to write and which this file's own imports make
    # available. Unparsing and taking the tail catches every spelling of the owner while
    # still requiring that it BE the config object, so `spec.month` or a local
    # `parsed.month` is untouched.
    substitutions = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Attribute)
        and node.attr == "month"
        and ast.unparse(node.value).rsplit(".", 1)[-1] in {"DEFAULT", "cfg"}
    ]
    assert not substitutions, (
        f"{script}.py main() reads the config's pinned month directly ("
        f"{len(substitutions)} occurrence(s)). That value equals the job YAMLs' own "
        "default, so substituting it changes nothing observable until the first run "
        "for another month -- and then it is wrong in the data, the checkpoint or a "
        "delete boundary, with nothing in the log naming the month that was used"
    )
