"""The shared loader mechanics: the month window, the row count, and the hash-key
expression's dependence on the spec.

WHY THIS FILE EXISTS AT ALL, since `test_cnpj_vault.py` drives all of it indirectly:
the Task 3 review found `_validated_months` and the missing-`_snapshot_month` refusal
UNREACHED -- `load_hub(months="2026-06")` was not asserted anywhere. Every refusal
here answers the same failure, and it is the one this layer is least able to notice:
a load that writes NOTHING and reports success looks exactly like a load that had
nothing to write.

THE THREE MONTH REFUSALS ARE NOT TESTED TWICE. They live in `opl.vault.months`, shared
with the observation ledger since the review flagged the duplication, and
`tests/vault/test_observation.py` already exercises them through the ledger. What is
asserted here is that the LOADER path reaches them -- which is a different claim, and
was the false one.

THE HUB PREFLIGHT IS ASSERTED HERE FOR THE SAME REASON AND IN THE SAME SHAPE.
`links.refuse_unloaded_hubs` is one guard with two callers -- `load_link` and
`load_partner_link` -- and its whole subject is a load that writes 28M rows of dangling
references and reports success. It is not in either link module's own test file because
those two files are the domain fixtures' (`test_estabelecimento_vault.py`,
`test_socios_vault.py`, at 734 and 795 lines against this project's 800-line cap), and a
guard shared by two loaders asserted in one of their files reads as that domain's
property. What the two domain files DO carry after this change is the ordering: their
spec refusals are handed hub tables nothing has loaded, so a preflight moved ahead of
`refuse_mismatched_hubs` turns them red."""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from opl.vault import domains
from opl.vault.hashing import hash_key
from opl.vault.hubs import load_hub
from opl.vault.links import load_link
from opl.vault.loading import (
    changed_rows,
    hash_key_expression,
    read_snapshot_window,
    rows_in,
)
from opl.vault.months import validated_months
from opl.vault.partners import load_partner_link
from opl.vault.registry import BusinessKeyColumn, Hub

from .conftest import LOADED_AT, MAY, write_delta

JUN, JUL = "2026-06", "2026-07"
_SCHEMA = "cnpj_basico string, _snapshot_month string, _snapshot_ref_date date"


@pytest.fixture(scope="module")
def snapshots(spark):
    """Two months of one column, as a temp view. No Delta write: nothing here reads
    enough rows for a managed table's reusable plan to pay for its ~22s of setup, which
    is the trade `tests/vault/test_observation.py::tables` measured in the other
    direction for a fixture that IS read heavily."""
    name = f"loading_{uuid4().hex[:8]}"
    spark.createDataFrame(
        [("10000001", JUN, date(2026, 6, 13)), ("10000002", JUL, date(2026, 7, 11))],
        _SCHEMA,
    ).createOrReplaceTempView(name)
    return name


def test_a_named_month_narrows_the_read_and_none_reads_everything(spark, snapshots):
    assert read_snapshot_window(spark, snapshots, [JUN]).count() == 1
    assert read_snapshot_window(spark, snapshots, [JUN, JUL]).count() == 2
    assert read_snapshot_window(spark, snapshots, None).count() == 2


def test_a_bare_string_month_is_refused(spark, snapshots):
    """`months="2026-06"` iterates to seven one-character strings, none of them a
    month, so the filter matches nothing and the load writes nothing -- successfully.
    No type checker catches it: `str` satisfies `Sequence[str]` structurally."""
    with pytest.raises(TypeError, match="bare str"):
        read_snapshot_window(spark, snapshots, "2026-06")


def test_an_empty_month_list_is_refused(spark, snapshots):
    with pytest.raises(ValueError, match="at least one month"):
        read_snapshot_window(spark, snapshots, [])


def test_a_value_that_is_not_a_month_is_refused(spark, snapshots):
    """Delegated to `opl.config.is_month`, the one spelling of that rule in this
    repository: `2026-13` has the shape and names no month."""
    with pytest.raises(ValueError, match="2026-13"):
        read_snapshot_window(spark, snapshots, [JUN, "2026-13"])


def test_a_source_with_no_snapshot_month_column_is_refused_when_a_window_is_named(
    spark
):
    """A table without `_snapshot_month` cannot be month-scoped, and the failure would
    otherwise be a Spark `AnalysisException` naming a column rather than saying that
    this source is not a monthly snapshot."""
    name = f"unmonthly_{uuid4().hex[:8]}"
    spark.createDataFrame([("10000001",)], "cnpj_basico string").createOrReplaceTempView(name)

    with pytest.raises(ValueError, match="_snapshot_month"):
        read_snapshot_window(spark, name, [JUN])


def test_a_source_with_no_snapshot_month_column_is_read_whole_when_no_window_is_named(
    spark
):
    """The refusal above is scoped to a NAMED window, which is what makes it a
    statement about the argument rather than about the table. `months=None` asks for
    everything and needs no month column to deliver it."""
    name = f"unmonthly_{uuid4().hex[:8]}"
    spark.createDataFrame([("10000001",)], "cnpj_basico string").createOrReplaceTempView(name)

    assert read_snapshot_window(spark, name, None).count() == 1


def test_the_month_consequence_reaches_the_message():
    """The shared validator takes its consequence from the caller so a loader's empty
    window does not tell an operator about an empty LEDGER. Asserted directly, because
    both callers pass a literal and nothing else would notice a swap."""
    with pytest.raises(ValueError, match="probe consequence"):
        validated_months([], column="_snapshot_month", consequence="probe consequence")


def test_a_count_of_a_table_that_does_not_exist_is_zero(spark):
    """`rows_in` is how both loaders learn what a target already held, and the first
    load of any vault table asks it about a table that is not there yet. Raising would
    make every table's first load fail."""
    assert rows_in(spark, f"nonexistent_{uuid4().hex[:8]}") == 0


def test_the_hash_key_expression_follows_the_specs_column_order(spark, snapshots):
    """Two hubs over the same columns in opposite order must produce DIFFERENT digests,
    because the standard length-prefixes each component and joins on `||`.

    The order is the spec's declaration order and that is not incidental -- it is
    written down once, in the domain module, and a hub that reordered its
    `business_keys` would re-key itself. Compared against `hash_key` on both sides, so
    this pins which order goes with which digest rather than only that they differ."""
    frame = spark.createDataFrame([("AB", "CD")], "left string, right string")
    forward = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="left"), BusinessKeyColumn(name="right")),
    )
    backward = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="right"), BusinessKeyColumn(name="left")),
    )

    row = frame.select(
        hash_key_expression(forward).alias("forward"),
        hash_key_expression(backward).alias("backward"),
    ).first()

    assert row["forward"] == hash_key(["AB", "CD"])
    assert row["backward"] == hash_key(["CD", "AB"])
    assert row["forward"] != row["backward"]


def test_a_business_key_with_no_width_is_not_padded(spark):
    """`width=None` means "take the value as it is", not "width unknown": padding a
    name or a free-text identifier would invent characters. Only a caller who knows the
    canonical width may assert one."""
    frame = spark.createDataFrame([("7",)], "k string")
    hub = Hub(name="h", hash_key="h_hk", business_keys=(BusinessKeyColumn(name="k"),))

    assert frame.select(hash_key_expression(hub).alias("d")).first()["d"] == hash_key(["7"])


# --------------------------------------------------------------------------- #
# `changed_rows`' own precondition, asserted on `changed_rows`.
# --------------------------------------------------------------------------- #

_CHANGED_SCHEMA = "k string, applied_date date, hash_diff string"
_JUN_REF, _JUL_REF = date(2026, 6, 13), date(2026, 7, 11)


def test_changed_rows_drops_a_candidate_its_target_already_holds(spark):
    """THE PRECONDITION THE DOCSTRING CALLED LOAD-BEARING, ASSERTED ON THE FUNCTION THAT
    OWNS IT rather than on the two loaders that used to satisfy it by hand.

    Both callers ran this anti-join themselves before calling, so the one step of
    `changed_rows`' contract that was NOT shared was the step its own docstring named as
    the reason for sharing. A third caller omitting it does not fail: two rows land at
    one position in the window, `lag` marks whichever it orders second as unchanged, and
    when the non-persisted one lands first it survives the filter and is appended again
    -- a duplicate on roughly half of re-runs, non-deterministically.

    `('A', JUN)` below is offered as a candidate AND is already persisted, so it must
    not come back. `('A', JUL)` must, because its digest differs from the persisted
    June row's -- which is also the assertion that `existing` still SEEDS the window
    rather than merely filtering it."""
    candidates = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d2"), ("B", _JUN_REF, "d3")],
        _CHANGED_SCHEMA,
    )
    existing = spark.createDataFrame([("A", _JUN_REF, "d1")], _CHANGED_SCHEMA)

    rows = changed_rows(candidates, existing, "k").collect()

    assert sorted((row["k"], row["applied_date"]) for row in rows) == [
        ("A", _JUL_REF), ("B", _JUN_REF)
    ]


def test_changed_rows_appends_nothing_when_every_candidate_is_already_persisted(spark):
    """Idempotence at the shared level: a re-run offers exactly what is on disk and
    gets back nothing. Asserted here as well as through the loaders because this is the
    property the anti-join exists for, and it now lives in one place."""
    rows = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d2")], _CHANGED_SCHEMA
    )

    assert changed_rows(rows, rows, "k").count() == 0


def test_changed_rows_with_no_target_treats_every_first_row_as_changed(spark):
    """`existing=None` is a first load, where there is nothing to drop and nothing to
    seed the window with. The contrast case, so the two assertions above read as the
    anti-join firing rather than as a constant."""
    candidates = spark.createDataFrame(
        [("A", _JUN_REF, "d1"), ("A", _JUL_REF, "d1"), ("B", _JUN_REF, "d3")],
        _CHANGED_SCHEMA,
    )

    rows = changed_rows(candidates, None, "k").collect()

    # ('A', JUL) repeats ('A', JUN)'s digest, so it is unchanged and dropped -- by the
    # `lag`, which is the OTHER half of this function and must still work untouched.
    assert sorted((row["k"], row["applied_date"]) for row in rows) == [
        ("A", _JUN_REF), ("B", _JUN_REF)
    ]


def test_the_loader_path_refuses_a_bare_string_month(spark, snapshots):
    """The refusals above are on `read_snapshot_window`; this is the one that says
    `load_hub` actually routes through it. The review's finding was precisely that
    `load_hub(months="2026-06")` was unasserted."""
    hub = Hub(
        name="h", hash_key="h_hk",
        business_keys=(BusinessKeyColumn(name="cnpj_basico", width=8),),
    )

    with pytest.raises(TypeError, match="bare str"):
        load_hub(
            spark, hub, source_table=snapshots, target_table="unused",
            load_date=datetime(2027, 1, 1), months="2026-06",
        )


# --------------------------------------------------------------------------- #
# The hub preflight, on both link loaders.
#
# THE DEFECT IT ANSWERS IS AN ORDERING BETWEEN TWO JOBS, WHICH IS WHY IT IS IN THE
# LOADER. `vault_partner_job.yml` loads `link_company_partner`, both of whose ends
# reference `hub_empresa` -- and that hub is `vault_empresa_job.yml`'s. A Databricks
# `depends_on` does not cross a job boundary, so on a fresh workspace the partner job can
# run first, and nothing in the write path notices: `partner_link_candidates` COMPUTES
# its references rather than joining to the hub, so 28M rows land pointing at hub rows
# that do not exist and the run reports success. On an insert-only table the repair is
# deleting rows by hand.
#
# WHAT IS ASSERTED IS "MISSING OR EMPTY", AND DELIBERATELY NOT MORE. Full referential
# integrity -- anti-joining every reference against the hub -- costs about an extra full
# pass, measured on this workspace at 2,606 s for `hub_empresa_from_estabelecimentos`
# anti-joining 144M rows to insert zero. There is no test below for a hub that is
# populated and missing SOME referenced key, because the guard does not catch that and a
# test claiming it did would be the same overstatement in another file.
# --------------------------------------------------------------------------- #

_PREFLIGHT_SCHEMA = (
    "cnpj_basico string, cnpj_ordem string, cnpj_dv string, "
    "identificador_socio string, cpf_cnpj_socio string, "
    "_snapshot_month string, _record_source string"
)
# ONE SOURCE CARRYING BOTH LINKS' KEY COLUMNS. Estabelecimentos and socios are two bronze
# tables in production; here they are one, because what is under test is a guard that
# never reads the source at all and a second fixture would be a second Delta write for
# nothing. Two rows, two distinct establishments and two distinct relationships.
_PREFLIGHT_ROWS = [
    ("10000001", "0001", "23", "2", "***111111**", JUN, "rfb_cnpj_webdav"),
    ("10000002", "0001", "45", "1", "90000001000199", JUL, "rfb_cnpj_webdav"),
]

ESTAB_LINK = domains.table_spec("link_empresa_estabelecimento")
ESTAB_LINK_HUBS = domains.linked_hubs(ESTAB_LINK)
PARTNER_LINK = domains.table_spec("link_company_partner")
PARTNER_LINK_HUBS = domains.linked_hubs(PARTNER_LINK)
EMPRESA_HUB, ESTABELECIMENTO_HUB = ESTAB_LINK_HUBS


def _estab_hubs(empresa: str, estabelecimento: str) -> dict[str, str]:
    """`link_empresa_estabelecimento`'s two hubs, keyed by hub NAME, which is how the
    preflight looks each one up. Spelled once so a test varies WHICH table a hub points
    at and nothing else."""
    return {EMPRESA_HUB.name: empresa, ESTABELECIMENTO_HUB.name: estabelecimento}


@pytest.fixture(scope="module")
def preflight(spark, vault_database):
    """A source, the two hubs really loaded from it, and a hub table that EXISTS AND IS
    EMPTY.

    THE EMPTY HUB IS MADE THE WAY AN OPERATOR MAKES ONE -- a real `load_hub` over a month
    this source does not hold. A hand-written zero-row table would test the same `count()`
    and would not be the thing the refusal message names, which is a hub whose own load
    ran over a window that matched nothing."""
    db = vault_database("link_preflight")
    source = f"{db}.source"
    write_delta(spark, source, _PREFLIGHT_SCHEMA, _PREFLIGHT_ROWS)
    names = SimpleNamespace(
        db=db, source=source, empresa=f"{db}.hub_empresa", empty=f"{db}.hub_empresa_empty",
        estabelecimento=f"{db}.hub_estabelecimento",
        never_created=f"{db}.hub_no_job_ever_loaded",
    )
    for hub, table in ((EMPRESA_HUB, names.empresa),
                       (ESTABELECIMENTO_HUB, names.estabelecimento)):
        load_hub(spark, hub, source_table=source, target_table=table, load_date=LOADED_AT)
    load_hub(spark, EMPRESA_HUB, source_table=source, target_table=names.empty,
             load_date=LOADED_AT, months=[MAY])
    return names


@pytest.fixture
def link_target(preflight):
    """A fresh link table per test. Sharing one would let "nothing was written" pass
    because an earlier test had not written either."""
    return f"{preflight.db}.link_{uuid4().hex[:8]}"


def test_a_link_load_refuses_a_hub_table_no_job_ever_created_and_writes_nothing(
    spark, preflight, link_target
):
    """The wrong-order run in its exact shape, on the generic loader.

    The assertion that matters is the second one: an exception alone would also be raised
    by a guard placed after the append, which is the version of this fix that fixes
    nothing."""
    with pytest.raises(ValueError, match="does not exist"):
        load_link(
            spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
            hub_tables=_estab_hubs(preflight.never_created, preflight.estabelecimento),
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(link_target)


def test_a_link_load_refuses_a_hub_table_that_exists_and_holds_no_rows(
    spark, preflight, link_target
):
    """THE HALF `tableExists` ALONE WOULD MISS, and it is not hypothetical: a hub job
    launched without `--params months=...` refuses on the sentinel, but one launched with
    a well-formed month the source does not hold writes an empty table and reports
    success. The link would then reference a hub that is there and holds nothing."""
    with pytest.raises(ValueError, match="holds no rows"):
        load_link(
            spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
            hub_tables=_estab_hubs(preflight.empty, preflight.estabelecimento),
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(link_target)


def test_a_link_load_writes_as_before_when_every_hub_it_references_is_populated(
    spark, preflight, link_target
):
    """The contrast case, without which the two refusals above are indistinguishable from
    a loader that refuses everything."""
    result = load_link(
        spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
        hub_tables=_estab_hubs(preflight.empresa, preflight.estabelecimento),
        source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
    )

    assert result.appended == 2
    assert spark.read.table(link_target).count() == 2


def test_a_hub_the_link_references_and_the_mapping_leaves_out_is_refused_by_name(
    spark, preflight, link_target
):
    """The mapping is keyed by hub name, so the way to get this wrong is to omit a hub
    rather than to misorder one -- and the hub omitted is exactly the one whose absence
    the preflight exists to find. Silently skipping it would make the guard weakest
    against the mistake nearest to it."""
    with pytest.raises(ValueError, match="must be named"):
        load_link(
            spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
            hub_tables={EMPRESA_HUB.name: preflight.empresa},
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(link_target)


def test_the_partner_link_loader_refuses_a_hub_no_job_ever_created_and_writes_nothing(
    spark, preflight, link_target
):
    """THE LOADER THE FINDING IS ABOUT. `link_company_partner` is self-referencing, so
    `hub_empresa` is both of its ends, and `vault_partner_job.yml` loads neither it nor
    anything else that would.

    The refusal also stands ahead of `_collapsed_duplicates`, a second full scan of the
    window, so the wrong-order run pays for nothing before being turned away."""
    with pytest.raises(ValueError, match="does not exist"):
        load_partner_link(
            spark, PARTNER_LINK, hubs=PARTNER_LINK_HUBS,
            hub_tables={EMPRESA_HUB.name: preflight.never_created},
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )

    assert not spark.catalog.tableExists(link_target)


def test_the_partner_link_loader_refuses_an_empty_hub_and_writes_when_it_is_populated(
    spark, preflight, link_target
):
    """Both arms on the derived loader, in one test, because the empty arm alone cannot
    tell a guard from a loader that stopped working."""
    with pytest.raises(ValueError, match="holds no rows"):
        load_partner_link(
            spark, PARTNER_LINK, hubs=PARTNER_LINK_HUBS,
            hub_tables={EMPRESA_HUB.name: preflight.empty},
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )
    assert not spark.catalog.tableExists(link_target)

    result = load_partner_link(
        spark, PARTNER_LINK, hubs=PARTNER_LINK_HUBS,
        hub_tables={EMPRESA_HUB.name: preflight.empresa},
        source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
    )

    assert result.appended == 2


def test_a_hub_gone_empty_between_two_loads_leaves_the_rows_already_written_untouched(
    spark, preflight, link_target
):
    """"BEFORE ANY WRITE" IS A CLAIM ABOUT A TARGET THAT ALREADY HAS ROWS, and a
    non-existent target cannot carry it: `saveAsTable` on an append that wrote zero rows
    still creates the table, so "the table is absent" and "the table is unchanged" are
    two different assertions and only the second survives a second load.

    A hub does not empty itself. What this stands in for is the incremental run -- the
    link already loaded, the hub since rebuilt or pointed at a window that matched
    nothing -- where a guard placed after the append would have appended first."""
    load_link(
        spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
        hub_tables=_estab_hubs(preflight.empresa, preflight.estabelecimento),
        source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
    )
    before = spark.read.table(link_target).count()

    with pytest.raises(ValueError, match="holds no rows"):
        load_link(
            spark, ESTAB_LINK, hubs=ESTAB_LINK_HUBS,
            hub_tables=_estab_hubs(preflight.empty, preflight.estabelecimento),
            source_table=preflight.source, target_table=link_target, load_date=LOADED_AT,
        )

    assert (before, spark.read.table(link_target).count()) == (2, 2)
