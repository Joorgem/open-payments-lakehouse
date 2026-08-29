"""What `opl.triage_agent.blast_radius` ANSWERS, and every guard that keeps it honest.

NO SPARK IN HERE, AND THE CLAIM IS NARROWER THAN IT LOOKS. Nothing in this file touches
`conftest.py`'s fixture machinery -- no `probe`, no `spark`, no view -- so every test runs
without a SparkSession, which is the 25-33 s this suite pays for one. It is NOT a claim that
pyspark is unimported: `opl.vault.domains` imports it at module scope and the subject module
therefore imports it transitively. Importing pyspark starts no JVM; asking for a session
does, and nothing here asks. NOTHING ENFORCES THAT, which is
`test_incidents_declaration.py`'s recorded position and its reason: every cheap spelling of
the guard passes while an autouse fixture or a transitive import still starts a JVM.

THE OTHER HALF IS `test_blast_radius_lock.py`, and the seam is what makes a test CHANGE.
Everything that reads `databricks/` -- the vault job YAMLs, the gold entry points -- is
there; everything about what the module DERIVES and PUBLISHES is here. The subject module
emits no SQL at all, so there is no Spark half to separate and the usual `_declaration`
split of this package does not apply. If a Spark test ever belongs to this subject it goes
in a NEW file, because the paragraph above is a property no test enforces.

THE TENSION THIS FILE IS SHAPED AROUND. A manifest that names EVERY downstream table and a
manifest that names the RIGHT ones both look like they worked: a blast radius returning all
six gold tables for every bronze table satisfies any assertion that only asks whether
something came back. So the assertions here are DIFFERENCES on real declarations -- exact
tuples, names that must be ABSENT, and one bronze table's answer held against another's --
and every guard is fired on data that breaks it rather than restated over data that already
passed at import.

EACH OF THE SEVEN CALLS AT THE FOOT OF THE MODULE HAS ITS OWN TEST, one mutation per call,
and that is a correction rather than a design: five of the seven could be deleted with this
suite green, including the one guarding the leg no sweep attests. A guard's BODY fired from
a test says nothing about whether the module still CALLS it. WHAT THOSE SEVEN TESTS DO NOT
HOLD: the ORDER of the calls, and the case of a NEW guard function added with no call at all
-- nothing here notices either.

THE TRAP THIS FILE OWNS IS THE THIRD ONE: two bronze tables reach gold with no vault table
in between, so a manifest walked bronze -> vault -> gold answers "nothing downstream" for
the workspace's largest incident. The defence is an import-time refusal, and what is
asserted here is that the refusal FIRES -- driven on a declaration missing that leg -- not
that the entry is present, which would pass whether or not anything defended it. The other
two traps are read off the bundle and are run in the lock file.
"""
from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, fields, replace

import pytest

from opl.bronze.registry import REGISTRY, UnknownTable
from opl.gold import registry as gold_registry_module
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.gold.specs import Scd2Dimension
from opl.triage_agent import blast_radius as blast_radius_module
from opl.triage_agent.blast_radius import (
    DIRECT_TO_GOLD,
    GOLD_WITHOUT_A_BRONZE_SOURCE,
    VAULT_LOADS_FROM,
    BlastRadius,
    _assert_every_gold_field_naming_a_gold_table_is_read,
    _assert_every_gold_field_naming_a_vault_table_is_read,
    _assert_no_bronze_table_reaches_nothing,
    _assert_the_direct_gold_declaration_names_registered_tables,
    _assert_the_unreached_gold_tables_are_the_declared_ones,
    _assert_the_vault_declaration_is_total_over_both_registries,
    _names_in,
    blast_radius,
    blast_radius_note,
    gold_sources_of,
    vault_sources_of,
)
from opl.vault import domains

# ----------------------------------------------------------------------------------
# Trap 2: the vault table with two bronze parents.
# ----------------------------------------------------------------------------------


def test_losing_the_second_parent_of_the_shared_hub_is_two_missing_gold_tables():
    """TRAP 2, RUN. A `dict[vault_table] = bronze_source` keeps whichever it saw last.

    Both single-parent spellings are driven, because which one survives depends on iteration
    order and NEITHER is safe: keep `empresas` and estabelecimentos loses the hub; keep
    `estabelecimentos` and empresas keeps `sat_empresa_dados` and so keeps `dim_company`
    anyway, which is the version that looks fine until somebody triages an estabelecimentos
    incident. The tuple-valued declaration is what makes both unspellable."""
    for kept, deprived in (("empresas", "estabelecimentos"), ("estabelecimentos", "empresas")):
        collapsed = {**VAULT_LOADS_FROM, "hub_empresa": (kept,)}
        radius = blast_radius_module._radius(
            deprived, loads_from=collapsed, direct=DIRECT_TO_GOLD
        )
        assert "hub_empresa" not in radius.vault
        assert "hub_empresa" in blast_radius(deprived).vault

    estab = blast_radius_module._radius(
        "estabelecimentos",
        loads_from={**VAULT_LOADS_FROM, "hub_empresa": ("empresas",)},
        direct=DIRECT_TO_GOLD,
    )
    assert set(blast_radius("estabelecimentos").gold) - set(estab.gold) == {
        "dim_company", "fact_payment",
    }


# ----------------------------------------------------------------------------------
# Trap 3: the two bronze tables that reach gold with no vault table in between.
# ----------------------------------------------------------------------------------


def test_the_headline_incidents_table_reaches_gold_with_no_vault_table_in_between():
    """`592660596679630` is a `payments` incident, the largest in this workspace, and the one
    whose recorded recommendation is DO NOT PROMOTE (`docs/f6-run-evidence.md` 0.3, 1.3).

    A manifest built by walking bronze -> vault -> gold answers "nothing downstream" for it.
    That answer raises nothing, is a perfectly ordinary shape for a bronze table here, and is
    the most reassuring thing this module could print. The exact tuples are asserted, not
    just non-emptiness, because "returned something" is satisfied by returning everything."""
    payments = blast_radius("payments")
    assert payments.vault == ()
    assert payments.gold == ("dim_date", "fact_payment")
    assert payments.bypasses_the_vault is True

    ptax = blast_radius("ptax")
    assert ptax.vault == ()
    assert ptax.gold == ("fact_payment",)
    assert ptax.bypasses_the_vault is True


def test_the_guard_refuses_a_declaration_that_drops_the_direct_to_gold_leg():
    """THE RED ARM. The defence is an import-time refusal, so this drives it on a
    declaration missing the entry rather than observing that the entry is there.

    Removing `payments` from `DIRECT_TO_GOLD` leaves it with no vault loader and no gold
    target -- an empty blast radius on the workspace's biggest incident -- and that has to
    be a raise. The control arm is first: the live declaration must pass, or the raise below
    could be about the injection rather than about the missing leg."""
    _assert_no_bronze_table_reaches_nothing(direct=DIRECT_TO_GOLD)

    with pytest.raises(ValueError, match="nothing downstream: \\['payments'\\]"):
        _assert_no_bronze_table_reaches_nothing(
            direct={key: value for key, value in DIRECT_TO_GOLD.items() if key != "payments"}
        )
    with pytest.raises(ValueError, match=r"nothing downstream: \['payments', 'ptax'\]"):
        _assert_no_bronze_table_reaches_nothing(direct={})


def test_the_guard_also_refuses_a_bronze_table_dropped_from_the_vault_declaration():
    """The same guard from the other side, so it is not a check about one dictionary.

    `socios` reaches no gold table at all, so its entire downstream set is its two vault
    tables; deleting them is the leg-1 shape of the same silence, and it must raise for the
    same reason `payments` does."""
    with pytest.raises(ValueError, match=r"nothing downstream: \['socios'\]"):
        _assert_no_bronze_table_reaches_nothing(
            loads_from={
                table: sources
                for table, sources in VAULT_LOADS_FROM.items()
                if "socios" not in sources
            }
        )


# ----------------------------------------------------------------------------------
# The differences: what each answer must NOT name.
# ----------------------------------------------------------------------------------


def test_each_bronze_tables_answer_is_its_own_and_not_the_whole_model():
    """THE ASSERTION THE WHOLE FILE EXISTS FOR: exact tuples, table by table.

    A blast radius returning all six gold tables and all eighteen vault tables for every
    bronze table would satisfy every non-emptiness check in this file. These are the numbers
    that cannot be produced that way -- THREE of the seven reach no gold table, TWO reach no
    vault table, and no two of the seven carry the same pair of legs."""
    answers = {table: blast_radius(table) for table in sorted(REGISTRY)}
    assert {table: radius.gold for table, radius in answers.items()} == {
        "empresas": ("dim_company", "dim_date", "fact_payment"),
        "estabelecimentos": ("dim_company", "fact_payment", "pit_estabelecimento"),
        "lookup": (),
        "merchant": (),
        "payments": ("dim_date", "fact_payment"),
        "ptax": ("fact_payment",),
        "socios": (),
    }
    assert {table: radius.vault for table, radius in answers.items()} == {
        "empresas": ("hub_empresa", "sat_empresa_dados"),
        "estabelecimentos": (
            "hub_empresa", "hub_estabelecimento", "link_empresa_estabelecimento",
            "sat_estabelecimento_dados", "sat_estabelecimento_endereco",
        ),
        "lookup": (
            "ref_cnae", "ref_motivo", "ref_municipio", "ref_natureza_juridica",
            "ref_pais", "ref_qualificacao",
        ),
        "merchant": (
            "hub_merchant", "link_merchant_empresa", "sat_eff_merchant_empresa",
            "sat_merchant_dados",
        ),
        "payments": (),
        "ptax": (),
        "socios": ("link_company_partner", "sat_eff_company_partner"),
    }


def test_the_gold_leg_excludes_by_name_the_tables_each_incident_must_not_be_sent_to():
    """The exclusions, stated as exclusions, because an equality on a tuple is easy to read
    as "these are present" and the point here is what is ABSENT.

    `estabelecimentos` does not reach `dim_date`: the calendar's satellite end is
    `sat_empresa_dados`, which only the empresas contract loads. `payments` does not reach
    `dim_company` or `pit_estabelecimento`, which are pure vault products. And nothing
    reaches the two enumerated dimensions."""
    assert "dim_date" not in blast_radius("estabelecimentos").gold
    assert "dim_date" in blast_radius("empresas").gold
    assert set(blast_radius("payments").gold).isdisjoint(
        {"dim_company", "pit_estabelecimento"}
    )
    assert "pit_estabelecimento" in blast_radius("estabelecimentos").gold
    for table in REGISTRY:
        assert set(blast_radius(table).gold).isdisjoint({"dim_channel", "dim_currency"})


def test_two_gold_tables_are_in_no_blast_radius_and_the_declaration_names_them():
    """An unreachable gold table is a correct answer here AND the shape of a stale
    derivation, so the expected set is written down and held equal to the graph."""
    reached = {name for table in REGISTRY for name in blast_radius(table).gold}
    assert set(GOLD_REGISTRY) - reached == {"dim_channel", "dim_currency"}
    assert set(GOLD_WITHOUT_A_BRONZE_SOURCE) == {"dim_channel", "dim_currency"}
    assert all(reason.strip() for reason in GOLD_WITHOUT_A_BRONZE_SOURCE.values())


def test_the_unreached_guard_catches_a_gold_table_the_derivation_stopped_seeing():
    """Fired by blinding the vault->gold derivation, which is exactly how leg 2 goes stale.

    With `vault_sources_of` returning nothing, `dim_company` and `pit_estabelecimento` fall
    out of every blast radius -- and out of nothing else, which is why an assertion over the
    OUTPUT would not notice. The control arm runs the guard unblinded first."""
    _assert_the_unreached_gold_tables_are_the_declared_ones()

    original = blast_radius_module.vault_sources_of
    blast_radius_module.vault_sources_of = lambda table: ()
    try:
        with pytest.raises(ValueError, match="dim_company"):
            _assert_the_unreached_gold_tables_are_the_declared_ones()
    finally:
        blast_radius_module.vault_sources_of = original

    assert blast_radius_module.vault_sources_of is original
    _assert_the_unreached_gold_tables_are_the_declared_ones()


# ----------------------------------------------------------------------------------
# The corpus: every incident gets an answer.
# ----------------------------------------------------------------------------------


def test_every_registered_bronze_table_has_a_non_empty_blast_radius():
    """Totality, which is the property the import guard refuses violations of.

    Like `tests/dataops/test_cadence.py`'s totality test this restates a refusal that
    already ran at import and therefore cannot fail here -- what CAN fail is
    `test_the_guard_refuses_a_declaration_that_drops_the_direct_to_gold_leg` above."""
    for table in REGISTRY:
        radius = blast_radius(table)
        assert radius.vault or radius.gold, table


def test_the_five_incidents_with_no_quarantine_evidence_still_get_a_blast_radius():
    """The one thing those five incidents CAN still be told, so it is asserted by name.

    `docs/f6-run-evidence.md` 0.5: three lookup incidents and two estabelecimentos ones hold
    zero quarantined rows and appear in no `dataops_reconciliation` row, so neither the
    census nor the verdict can say anything about them. Their TABLE is still known --
    declared, not inferred (0.6) -- and a table is all a blast radius needs, so this is the
    layer of the report that survives the evidence being gone.

    THE TWO ANSWERS ARE DIFFERENT, which is the discriminating half: a lookup incident is a
    reference-table story and an estabelecimentos one reaches the star."""
    assert blast_radius("lookup").vault == (
        "ref_cnae", "ref_motivo", "ref_municipio", "ref_natureza_juridica", "ref_pais",
        "ref_qualificacao",
    )
    assert blast_radius("lookup").gold == ()
    assert blast_radius("estabelecimentos").gold == (
        "dim_company", "fact_payment", "pit_estabelecimento",
    )


def test_every_table_the_incident_corpus_names_gets_an_answer():
    """The eleven incidents wear five distinct tables (`docs/f6-run-evidence.md` 0.3), and
    every one of them resolves. The list is the corpus's, not the registry's, so it is a
    statement about the workspace rather than a restatement of totality."""
    corpus = ("payments", "socios", "estabelecimentos", "empresas", "lookup")
    assert set(corpus) <= set(REGISTRY)
    for table in corpus:
        assert blast_radius_note(table).startswith(table)


def test_an_incident_whose_source_the_declaration_does_not_know_is_refused():
    """T1 emits a NULL `source` for a gate that fired on a job it does not declare, so this
    is a real row of the feed rather than a caller's mistake -- and inventing an empty blast
    radius for it would render a stale declaration exactly like a bronze table with nothing
    downstream, which is the answer this whole module is built to make impossible.

    THE REFUSAL IS `evidence.py`'s, IMPORTED RATHER THAN RE-SPELLED, which the message
    below is what proves: it names `incident_feed_sql`, a function this module does not
    know about."""
    for absent in (None, "", "   "):
        with pytest.raises(UnknownTable, match="incident_feed_sql"):
            blast_radius(absent)
    with pytest.raises(UnknownTable, match="unknown bronze table"):
        blast_radius("bronze_payments")


# ----------------------------------------------------------------------------------
# Leg 2: the derivation over the gold registry, and the guards that keep it honest.
# ----------------------------------------------------------------------------------


def test_the_scd2_derivation_returns_the_hub_the_loader_reads_and_no_field_names():
    """`gold_load_dimension.py` resolves `domains.parent_hub(satellite)` and reads BOTH
    tables; the hub is in no field of any gold spec, so a derivation over field values alone
    returns half the answer and `dim_company` stops being reachable from `estabelecimentos`
    -- whose only path to it is through `hub_empresa`."""
    assert vault_sources_of(GOLD_REGISTRY["dim_company"]) == (
        "hub_empresa", "sat_empresa_dados",
    )
    assert vault_sources_of(GOLD_REGISTRY["dim_date"]) == ("sat_empresa_dados",)
    assert vault_sources_of(GOLD_REGISTRY["pit_estabelecimento"]) == (
        "hub_estabelecimento", "sat_estabelecimento_dados", "sat_estabelecimento_endereco",
    )
    assert vault_sources_of(GOLD_REGISTRY["dim_channel"]) == ()
    assert vault_sources_of(GOLD_REGISTRY["fact_payment"]) == ()


def test_the_sweep_the_two_gold_field_guards_run_on_reads_into_tuples():
    """WHAT THE TWO GUARDS BELOW ARE ALLOWED TO SEE, held against the registry they sweep.

    `_names_in` reads one level into tuples, and dropping that branch changes no answer
    TODAY -- every tuple-carried name the sweep would lose is already returned by
    `vault_sources_of` or `gold_sources_of`, so both guards stay silent either way. What it
    changes is what the guards RANGE OVER: a scalars-only sweep cannot see a vault or gold
    table named in a tuple field, which is the one place a new source field is most likely
    to arrive -- `fact_payment.conformed` and `pit_estabelecimento`'s satellites are both
    tuples already.

    IT IS COMPUTED AND NOT LISTED. `carried` re-derives the tuple-borne strings from the
    dataclass fields, so this asserts a relation between two readings of the live registry
    rather than a copy of today's names, and the first assertion is the control: if no gold
    spec carried a tuple of strings any more, the branch would be unreachable and this test
    would be proving nothing."""
    def carried(table) -> set[str]:
        return {
            item
            for spec in fields(table)
            if isinstance(getattr(table, spec.name), tuple)
            for item in getattr(table, spec.name)
            if isinstance(item, str)
        }

    exercised = {name for name, table in GOLD_REGISTRY.items() if carried(table)}
    assert exercised, "no gold spec carries a tuple of strings -- the branch is unreachable"

    for name, table in GOLD_REGISTRY.items():
        assert carried(table) <= _names_in(table), f"{name}: a tuple-borne name is unswept"


def test_the_star_edges_are_the_facts_own_declaration_and_nothing_elses():
    """The closure follows `company_dimension` and `conformed` and nothing else, which is
    what carries an `empresas` incident from `dim_company` to `fact_payment`."""
    assert gold_sources_of(GOLD_REGISTRY["fact_payment"]) == (
        "dim_company", "dim_date", "dim_channel", "dim_currency",
    )
    assert gold_sources_of(GOLD_REGISTRY["dim_company"]) == ()


@dataclass(frozen=True, kw_only=True)
class _AKindTheDerivationCannotSee:
    """A gold kind carrying a source field neither derivation knows about.

    A THROWAWAY SPEC, which is `opl.gold.registry.build_registry`'s pattern: the guards
    exist for a kind that does not exist yet, so the only way to watch them fail is to
    invent one. It is a dataclass because `_names_in` reads `dataclasses.fields`."""

    name: str
    source_satellite_the_second: str = "sat_merchant_dados"
    partner_dimension: str = "dim_company"


def test_the_vault_field_guard_catches_a_gold_kind_whose_source_it_cannot_see():
    """Leg 2's staleness, fired. A new kind falls through `vault_sources_of` to `()`, its
    source drops out of every blast radius, and NOTHING ELSE CHANGES -- no exception, no
    NULL, no shorter list anywhere a test is looking.

    The control arm runs the guard over the live registry first, so the raise is about the
    added kind and not about the guard."""
    _assert_every_gold_field_naming_a_vault_table_is_read()

    stub = _AKindTheDerivationCannotSee(name="dim_something")
    with pytest.raises(ValueError, match="sat_merchant_dados"):
        _with_registry({**GOLD_REGISTRY, "dim_something": stub})(
            _assert_every_gold_field_naming_a_vault_table_is_read
        )


def test_the_gold_field_guard_catches_a_star_edge_the_closure_would_not_follow():
    """The mirror, and it fails differently: a missed vault source drops one table from an
    answer, a missed gold source truncates the closure and drops everything below it. Two
    guards rather than one check written twice."""
    _assert_every_gold_field_naming_a_gold_table_is_read()

    stub = _AKindTheDerivationCannotSee(name="dim_something")
    with pytest.raises(ValueError, match="dim_company"):
        _with_registry({**GOLD_REGISTRY, "dim_something": stub})(
            _assert_every_gold_field_naming_a_gold_table_is_read
        )


def _with_registry(registry):
    """Run a zero-argument guard against a substituted `GOLD_REGISTRY`, then put it back.

    `GOLD_REGISTRY` is a `MappingProxyType`, so it cannot be mutated in place; the module
    global is rebound instead and restored in a `finally`, because a test that left the
    registry substituted would break every test collected after it."""

    def run(guard):
        original = blast_radius_module.GOLD_REGISTRY
        blast_radius_module.GOLD_REGISTRY = registry
        try:
            guard()
        finally:
            blast_radius_module.GOLD_REGISTRY = original

    return run


def test_the_vault_declaration_is_total_over_both_registries():
    """Restates a refusal that already ran at import and so cannot fail here; what can fail
    is the guard driven on a broken declaration, which is the next test."""
    assert set(VAULT_LOADS_FROM) == set(domains.REGISTRY)
    assert {source for sources in VAULT_LOADS_FROM.values() for source in sources} <= set(
        REGISTRY
    )
    _assert_the_vault_declaration_is_total_over_both_registries()


def test_the_totality_guard_catches_a_vault_table_no_bronze_table_loads(monkeypatch):
    """A vault table nothing declares a parent for is one no incident ever names, so every
    gold table built on it falls out of every answer. Both shapes are fired: a missing key
    and -- the one a key-set check would pass -- an entry naming no parent at all."""
    monkeypatch.setattr(
        blast_radius_module,
        "VAULT_LOADS_FROM",
        {table: sources for table, sources in VAULT_LOADS_FROM.items() if table != "ref_pais"},
    )
    with pytest.raises(ValueError, match=r"no declared source \['ref_pais'\]"):
        _assert_the_vault_declaration_is_total_over_both_registries()

    monkeypatch.setattr(
        blast_radius_module, "VAULT_LOADS_FROM", {**VAULT_LOADS_FROM, "ref_pais": ()}
    )
    with pytest.raises(ValueError, match=r"no parent at all \['ref_pais'\]"):
        _assert_the_vault_declaration_is_total_over_both_registries()


# ----------------------------------------------------------------------------------
# The record's shape, the note, and the import-time calls.
# ----------------------------------------------------------------------------------


def test_the_record_carries_no_count_no_proportion_and_no_score():
    """THE HARD RULE, PINNED STRUCTURALLY rather than by a comment saying not to.

    A blast radius by proportion classifies `socios` near 100% in
    `tests/triage_agent/conftest.py` -- 1,800 staged rows -- and near 0% against the
    55,830,826 rows of the live STAGING table, with no test able to tell the two apart.
    `severity.py` refused the same ratio for the same measurement; the record
    holds names and nothing that could be summed, ranked or rendered as a percentage, and
    the field list is asserted EXACTLY: an added `affected_rows` fails here."""
    assert [item.name for item in fields(BlastRadius)] == ["source", "vault", "gold"]
    for table in REGISTRY:
        radius = blast_radius(table)
        assert isinstance(radius.source, str)
        assert all(isinstance(name, str) for name in (*radius.vault, *radius.gold))


def test_the_vault_bypass_needs_both_halves_and_not_just_an_empty_vault_leg():
    """`bypasses_the_vault` on a record that reaches nothing at all must be False.

    That record cannot come out of `blast_radius` -- the import guard refuses it -- so it is
    built here directly, which is the only way to fire the second half of the expression.
    Written as `not self.vault` alone the property would be True for it, and "reaches gold
    without the vault" would be the word printed for a table that reaches nothing."""
    assert BlastRadius(source="x", vault=(), gold=()).bypasses_the_vault is False
    assert BlastRadius(source="x", vault=(), gold=("g",)).bypasses_the_vault is True
    assert BlastRadius(source="x", vault=("v",), gold=("g",)).bypasses_the_vault is False


def test_the_note_has_three_arms_and_a_real_table_reaches_each():
    """An arm no input can reach is this repository's most-hunted species, so each is named
    with the table that takes it. The first arm is the one that matters: it says the bypass
    OUT LOUD rather than leaving an empty vault list to be read as an empty answer."""
    assert "NO vault loader task" in blast_radius_note("payments")
    assert "fact_payment" in blast_radius_note("ptax")
    assert "and through them" in blast_radius_note("empresas")
    assert "NO gold table" in blast_radius_note("merchant")
    assert "NO gold table" in blast_radius_note("socios")
    assert "NO vault loader task" not in blast_radius_note("lookup")


def _reimported_blast_radius():
    """A SECOND execution of `blast_radius.py`'s module body, from its own file.

    Not `importlib.reload`, which would rebind the module every other test imported from.
    This builds a throwaway module, runs the body, and is the only way to observe what the
    import-time calls do.

    REGISTERED UNDER A THROWAWAY NAME AND REMOVED AGAIN, which is `test_history_declaration
    .py`'s `_executed` and its reason verbatim: the subject module declares a `@dataclass`
    under `from __future__ import annotations`, so `dataclasses` resolves the string
    annotations by looking the defining class's `__module__` up in `sys.modules` and raises
    `AttributeError` on a module that is not there. `test_incidents_declaration.py` gets
    away with the shorter spelling because `incidents.py` declares no dataclass -- which is
    a fact about that module and not a technique, and was measured again here."""
    spec = importlib.util.spec_from_file_location(
        "opl.triage_agent._blast_radius_reimported", blast_radius_module.__file__
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        del sys.modules[spec.name]
    return module


def test_the_guards_run_at_import_so_deleting_a_call_is_a_failure_not_a_silent_loss(
    monkeypatch,
):
    """CALL 4 OF THE SEVEN, and only that one -- the other six have a test each below and in
    the section that follows. Calling a guard from a test only restates its body over data
    that already passed at import; it says nothing about whether the module still CALLS it.
    Deleting the calls at the foot of `incidents.py` left this suite green once, which is
    how that was learned, and five of these seven were deletable here until this pass.

    `REGISTRY` gains a bronze table nothing loads and nothing reads, the module body is
    executed again, and the ValueError has to come out of the IMPORT. The first line is the
    control: re-executing an unmutated module must succeed.

    THE ADDED SPEC IS `replace`d SO ITS NAME IS ITS KEY, which is not tidiness: handed
    `ptax`'s spec unchanged, the added entry answers to `'ptax'` and the FIRST guard in the
    foot fires on the key/name mismatch instead -- a raise carrying the same string, from a
    different refusal, which would have made this test pass while proving something else."""
    assert _reimported_blast_radius().VAULT_LOADS_FROM == VAULT_LOADS_FROM

    orphan = "a_table_nothing_downstream_reads"
    monkeypatch.setitem(REGISTRY, orphan, replace(REGISTRY["ptax"], name=orphan))
    with pytest.raises(ValueError, match=f"nothing downstream: \\['{orphan}'\\]"):
        _reimported_blast_radius()


def test_the_key_and_name_guard_catches_a_bronze_table_answering_to_two_strings(monkeypatch):
    """CALL 1 OF THE SEVEN, and its body's only test as well. The declarations are written
    in `REGISTRY` KEYS and the lookup resolves to `spec.name`; a table for which those
    differ falls out of one namespace silently, so the module refuses the state rather than
    picking a side.

    It is fired here because a guard nobody has watched fail is a guard nobody has tested --
    this one cannot be reached by any registry the repository ships, which is exactly the
    reason to build the registry that reaches it."""
    monkeypatch.setitem(REGISTRY, "answers_to_two_strings", replace(REGISTRY["ptax"], name="x"))
    with pytest.raises(ValueError, match="registry key is not their name"):
        _reimported_blast_radius()


# ----------------------------------------------------------------------------------
# The other five of the seven calls at the foot, one mutation each.
# ----------------------------------------------------------------------------------


def test_the_direct_gold_guard_refuses_the_typo_that_drops_the_stars_only_fact(monkeypatch):
    """THE BODY NOTHING HAD EVER FIRED, and it guards the one leg no sweep attests.

    `"fact_payments"` for `"fact_payment"` does not crash and does not read as a typo: it
    reads as a gold table that simply is not reached, and the table it silently drops is the
    star's only fact, out of `592660596679630`. All three arms are driven -- they fail on
    different halves of the declaration, and the empty-target arm is the one a check on the
    KEY SET passes. The control arm is first, so each raise is about the injected
    declaration rather than about the guard."""
    _assert_the_direct_gold_declaration_names_registered_tables()

    for drifted, message in (
        (
            {**DIRECT_TO_GOLD, "payments": ("dim_date", "fact_payments")},
            r"unregistered gold targets \['fact_payments'\]",
        ),
        (
            {**DIRECT_TO_GOLD, "bronze_payments": ("fact_payment",)},
            r"unregistered bronze sources \['bronze_payments'\]",
        ),
        ({**DIRECT_TO_GOLD, "payments": ()}, r"sources declaring no target \['payments'\]"),
    ):
        monkeypatch.setattr(blast_radius_module, "DIRECT_TO_GOLD", drifted)
        with pytest.raises(ValueError, match=message):
            _assert_the_direct_gold_declaration_names_registered_tables()


def test_the_vault_totality_call_is_at_the_foot_and_a_lost_parent_fails_the_import(
    monkeypatch,
):
    """CALL 2. `socios` leaves the bronze registry, so `link_company_partner` and
    `sat_eff_company_partner` declare a parent nothing registers.

    THE MUTATION TRIPS THIS GUARD AND NO OTHER, which is what makes it a statement about the
    CALL: `socios` is in no `DIRECT_TO_GOLD` entry, reaches no gold table, and is not a
    registry key any guard below iterates once it is gone -- so with the call deleted the
    module body runs to the end and this is the only test that notices."""
    monkeypatch.delitem(REGISTRY, "socios")
    with pytest.raises(ValueError, match="parents no bronze registry knows"):
        _reimported_blast_radius()


def test_the_direct_gold_call_is_at_the_foot_and_an_unregistered_source_fails_the_import(
    monkeypatch,
):
    """CALL 3, the one that had neither a call test nor a body test until this pass.

    `ptax` leaves the bronze registry and `DIRECT_TO_GOLD` still names it. Guard 2 is quiet
    because `ptax` is the parent of no vault table; guard 5 is quiet because the only gold
    table `ptax` ever reached is `fact_payment`, which `payments` reaches as well."""
    monkeypatch.delitem(REGISTRY, "ptax")
    with pytest.raises(ValueError, match=r"unregistered bronze sources \['ptax'\]"):
        _reimported_blast_radius()


@dataclass(frozen=True, kw_only=True)
class _AGoldTableNothingCanReach:
    """A gold spec carrying ONE field, and its value is the table's own name.

    Deliberately naming neither a vault table nor another gold table: a mutation that trips
    a second guard proves nothing about the call it was aimed at, and the two guards after
    this one read exactly those two namespaces."""

    name: str


def test_the_unreached_gold_call_is_at_the_foot_and_an_orphan_gold_table_fails_the_import(
    monkeypatch,
):
    """CALL 5. A gold table no bronze table reaches, and nothing declares unreachable, is
    indistinguishable from the outside from a vault->gold derivation that has gone stale --
    so the import refuses it instead of letting it drop out of every answer in silence."""
    stub = _AGoldTableNothingCanReach(name="dim_nothing_can_reach_this")
    monkeypatch.setattr(
        gold_registry_module, "REGISTRY", {**GOLD_REGISTRY, stub.name: stub}
    )
    with pytest.raises(ValueError, match=r"explains \['dim_nothing_can_reach_this'\]"):
        _reimported_blast_radius()


@dataclass(frozen=True, kw_only=True)
class _AnScd2NamingAVaultTableTheDerivationDoesNotRead(Scd2Dimension):
    """An SCD2 dimension with a SECOND satellite field, which `vault_sources_of` cannot see."""

    second_source_satellite: str = "sat_merchant_dados"


@dataclass(frozen=True, kw_only=True)
class _AnScd2NamingAGoldTableTheClosureDoesNotFollow(Scd2Dimension):
    """An SCD2 dimension naming another GOLD table, which `gold_sources_of` cannot see."""

    partner_dimension: str = "dim_date"


def _dim_company_as(kind: type[Scd2Dimension]) -> Scd2Dimension:
    """`dim_company` rebuilt under `kind`, field for field.

    IT HAS TO BE AN EXISTING, REACHED TABLE rather than an added one: a new gold table is
    reached by nothing, which trips the unreached-gold guard FIRST and would leave the two
    tests below passing with the calls they are aimed at deleted."""
    base = GOLD_REGISTRY["dim_company"]
    return kind(**{item.name: getattr(base, item.name) for item in fields(base)})


def test_the_vault_field_call_is_at_the_foot_and_an_unseen_source_field_fails_the_import(
    monkeypatch,
):
    """CALL 6. `dim_company` gains a second satellite the derivation does not look at, which
    is how leg 2 goes stale: the vault table stops being named and nothing else changes.

    `sat_merchant_dados` is a VAULT table and not a gold one, so guard 7 stays quiet."""
    stub = _dim_company_as(_AnScd2NamingAVaultTableTheDerivationDoesNotRead)
    monkeypatch.setattr(
        gold_registry_module, "REGISTRY", {**GOLD_REGISTRY, "dim_company": stub}
    )
    with pytest.raises(ValueError, match="naming a vault table that"):
        _reimported_blast_radius()


def test_the_gold_field_call_is_at_the_foot_and_an_unfollowed_star_edge_fails_the_import(
    monkeypatch,
):
    """CALL 7, the mirror, and it fails differently: a missed vault source drops one table
    from an answer, a missed gold source truncates the closure and drops everything below
    it.

    `dim_date` is a GOLD table and not a vault one, so guard 6 stays quiet."""
    stub = _dim_company_as(_AnScd2NamingAGoldTableTheClosureDoesNotFollow)
    monkeypatch.setattr(
        gold_registry_module, "REGISTRY", {**GOLD_REGISTRY, "dim_company": stub}
    )
    with pytest.raises(ValueError, match="naming another gold table that"):
        _reimported_blast_radius()
