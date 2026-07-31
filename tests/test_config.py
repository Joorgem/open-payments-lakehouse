# tests/test_config.py
import pytest

from opl.config import DEFAULT, OplConfig, require_month


def test_defaults_match_free_edition_layout():
    assert DEFAULT.catalog == "workspace"
    assert DEFAULT.schema == "default"
    assert DEFAULT.volume_root == "/Volumes/workspace/default/landing"
    assert DEFAULT.landing_cnpj_root == "/Volumes/workspace/default/landing/cnpj"


def test_month_path_and_table_helpers():
    assert DEFAULT.landing_cnpj_month() == (
        "/Volumes/workspace/default/landing/cnpj/2026-06"
    )
    assert DEFAULT.landing_cnpj_month("2026-07") == (
        "/Volumes/workspace/default/landing/cnpj/2026-07"
    )
    assert DEFAULT.table("bronze_cnpj_lookup") == (
        "workspace.default.bronze_cnpj_lookup"
    )


def test_the_unzip_staging_dir_is_outside_every_dir_an_auto_loader_reads():
    """`landing_tmp` exists so a half-written file is never created where a stream
    can see it. Every stream reads its own `landing_table(...)` subdir with no glob,
    and cloudFiles walks a source dir recursively (empirically, an F1.3 probe planted
    in `zips/` was ingested by a stream reading the month root). Being outside the
    month root clears every source path without relying on a glob -- while staying
    inside `volume_root`, i.e. inside the one UC Volume, which is what lets os.replace
    rename out of it into the landing dir (a cross-filesystem replace raises EXDEV)."""
    staging = DEFAULT.landing_tmp("estabelecimentos", "2026-07")

    assert staging == "/Volumes/workspace/default/landing/_tmp/cnpj/2026-07/estabelecimentos"
    assert not staging.startswith(DEFAULT.landing_table("estabelecimentos", "2026-07"))
    assert not staging.startswith(DEFAULT.landing_cnpj_month("2026-07"))
    assert not staging.startswith(DEFAULT.landing_cnpj_root)
    assert staging.startswith(DEFAULT.volume_root)  # same Volume => same filesystem
    assert DEFAULT.landing_tmp("estabelecimentos").endswith("/2026-06/estabelecimentos")


def test_is_frozen():
    import dataclasses
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        OplConfig().catalog = "other"  # type: ignore[misc]


# --- require_month: the shared guard four entry points route their month through ---

@pytest.mark.parametrize("action", ["ingest", "reclaim"])
@pytest.mark.parametrize("month", [None, "", "  "])
def test_an_absent_month_is_refused_and_never_defaulted(month, action):
    """The lock that matters, and the mirror of
    `test_the_snapshot_month_has_no_default` in tests/bronze/test_autoloader_helpers.py.

    Same question, same answer, now in one place: this guard must have NO default,
    because both candidates are wrong. `opl.config`'s pinned month is how F1.2 tied
    every row to 2026-06 silently, and the current month invents a fact.

    Four entry points substituted `DEFAULT.month` before this existed, and the two
    ingest tasks are the sharper case: their local goes straight to
    `add_audit_columns(snapshot_month=...)`, the parameter given no default
    precisely so that value could not be supplied silently. The guard was satisfied
    by the one value it exists to refuse -- decorative, which is worse than absent,
    because the next reader sees it and believes the hole is closed.

    Absence is asserted apart from malformation because the failure it prevents is
    the INVISIBLE one: the pinned month equals the job YAMLs' own default, so an
    omission changes nothing observable until the first run for another month. The
    message is asserted, not just the raise -- a refusal that does not name the
    missing parameter and where it comes from leaves the operator exactly where the
    silent default did. `action` is parametrized because the message has to name the
    task that is refusing, or it misreports where the run stopped."""
    with pytest.raises(ValueError, match="no month was given") as excinfo:
        require_month(month, action=action)
    message = str(excinfo.value)
    assert f"refusing to {action}" in message
    assert "{{job.parameters.month}}" in message
    assert "no default" in message


@pytest.mark.parametrize(
    "month",
    ["2026-06/zips", "2026-06/..", "..", ".", "2026-6", "26-06",
     "2026-06 ", "/2026-06", "2026-06\\zips", "2026-06/estabelecimentos"],
)
def test_a_month_that_is_not_a_month_is_refused(month):
    """The month is not decoration: it is interpolated RAW into the landing path.

    `landing_table(subdir, month)` builds that path, so `month="2026-06/zips"` makes
    it `cnpj/2026-06/zips/<table>` -- the ZIPS DIRECTORY ITSELF. For a reclaim that
    moves the containment root onto the archives and every zip under it then reads
    as "inside"; for an ingest it points the stream at them. The zips are the only
    way back to the source when a parse defect is found after ingestion, which
    happened twice in F1.3.

    Not a hypothetical input on the reclaim side: bronze rows whose `_source_file`
    sits under `zips/` are what a pre-Task-8 month-root stream produced, which is
    the documented F1.3 probe.txt mechanism. The whole point of the containment
    guard is to be INDEPENDENT of whether bronze holds such rows, and an
    unvalidated month re-couples it to that one assumption."""
    with pytest.raises(ValueError, match="month"):
        require_month(month, action="reclaim")


@pytest.mark.parametrize("month", ["2026-13", "2026-00", "2026-99", "0000-13"])
def test_a_shape_valid_month_naming_no_real_month_is_refused(month):
    """The half a `\\d{4}-\\d{2}` shape check cannot see, and the reason it matters.

    These are not path traversal -- they are WRONG BUT WELL FORMED, which is the
    failure this branch keeps re-finding. `2026-13` clears the shape check and names
    no directory that can exist, so `reclaim_landing` scopes its containment root to
    `cnpj/2026-13/estabelecimentos`, every file bronze proved reads as OUTSIDE it, and
    the task prints `REFUSED (left untouched)` for all of them and exits green. That
    log is indistinguishable from the containment guard catching a real F1.3
    probe.txt incident, so the operator's next move is to investigate a leak that
    never happened while the month bug goes unnamed.

    Refused HERE rather than only in `opl.bronze.snapshot.ref_date_column`, which had
    the only copy of `1 <= int(month) <= 12`: that function is reached by the two
    ingest tasks and by neither `unzip_table` nor `reclaim_landing`, so before this
    the same value was refused at two of the four entry points and accepted at the
    two that pick a delete boundary. Range and shape are now one rule in one place
    (`is_month`), which is the property
    `test_the_calendar_range_has_exactly_one_spelling` below pins."""
    with pytest.raises(ValueError, match="month") as excinfo:
        require_month(month, action="reclaim")
    assert "01-12" in str(excinfo.value)


def test_the_calendar_range_has_exactly_one_spelling():
    """`opl.bronze.snapshot` must ASK the rule, not restate it.

    A source-text assertion, like `test_ingest_tasks_batch_id`'s check that the job
    tasks still route their month through `require_month`, and for the same reason:
    what broke was not the logic in either place but the existence of two places. The
    range check lived only in `ref_date_column`, which the two ingest tasks reach and
    the unzip and reclaim tasks do not, so `2026-13` was refused for half the entry
    points. Re-introducing a local `<= 12` there would restore exactly that split
    while every test in `tests/bronze/test_snapshot.py` still passed."""
    import ast
    from pathlib import Path

    source = (
        Path(__file__).resolve().parents[1] / "src" / "opl" / "bronze" / "snapshot.py"
    ).read_text(encoding="utf-8")
    fn = next(
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.FunctionDef) and node.name == "ref_date_column"
    )
    # Docstring dropped, because it QUOTES the old check to explain why it moved --
    # asserting over the prose would fail on the very comment that records the fix.
    statements = fn.body[1:] if ast.get_docstring(fn) else fn.body
    body = "\n".join(ast.unparse(s) for s in statements)

    assert "is_month(snapshot_month)" in body, (
        "ref_date_column no longer asks opl.config.is_month for the month rule"
    )
    assert "<= 12" not in body, (
        "the calendar range has a second spelling inside ref_date_column -- that "
        "split is what let 2026-13 through unzip_table and reclaim_landing"
    )


def test_a_real_month_is_accepted_unchanged():
    assert require_month("2026-06", action="ingest") == "2026-06"
    assert require_month("2026-12", action="ingest") == "2026-12"
    assert require_month("2026-01", action="ingest") == "2026-01"
