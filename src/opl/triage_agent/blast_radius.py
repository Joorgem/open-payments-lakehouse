# src/opl/triage_agent/blast_radius.py
"""WHICH tables a gated bronze table feeds, downstream. NEVER HOW MUCH OF THEM.

WHAT THIS ANSWERS. `incidents.py` says which job runs are being triaged and which bronze
table each one is about; `evidence.py` says what is in the workspace for it; `severity.py`
grades it; `history.py` says how much comparable history exists. None of them says what
ELSE is downstream of the table that was gated -- which is the first question a person asks
after "how bad is it", because it decides whether a stranded batch is a bronze-local
problem or one that has already been read by a dimension somebody is reporting off.

IT READS NOTHING AND WRITES NOTHING, not even a query. Every input is in this repository:
the bundle declares which bronze table each vault loader task reads, and `opl.gold.registry`
declares which vault table each gold table is built from. So this module is a lookup over
declared data plus a derivation over a registry the wheel already carries, and the package's
"reads, ranks and drafts" rule (ADR 0018 Decision 3) is satisfied by there being nothing to
read. See "NO SQL" at the foot of this docstring for why that is not an inconsistency with
its four siblings.

WHICH TABLES, NEVER HOW MUCH, AND THAT IS MEASURED RATHER THAN TASTEFUL. A blast radius
expressed as a PROPORTION -- "this incident touches 43% of the rows in the star" -- is the
one shape this corpus cannot carry, and `severity.py` has already paid for finding that out:
its "WHAT SEVERITY DELIBERATELY DOES NOT READ" opens by refusing staged/promoted/quarantined
AS A RATIO, because `tests/triage_agent/conftest.py` sets socios to 1,800 staged against a
live staging table of 55,830,826 rows -- counts chosen to make each batch RECONCILE rather
than to reproduce a measurement, with only `592660596679630`'s being the workspace's own. A
ratio therefore ranks socios near 100% in the fixture and near 0% on the deploy, WITH NO
TEST ABLE TO TELL THE TWO APART. The phase plan predicted this module would meet the same
wall, and it does: nothing here is a count, a percentage or a score. The answer is a list of
table names, and a reader who wants a magnitude reads `evidence.py`'s census, which counts
rows that exist.

A COUNT OF TABLES IS NOT AN EXCEPTION TO THAT AND IS DELIBERATELY NOT PUBLISHED EITHER.
`len(radius.gold)` is one line away for any caller who wants it, and it is not a column here
because "4 tables affected" beside "2 tables affected" is a ranking, and ranking is
`severity.py`'s and stays there. Fusing the two would put a bronze table's position in the
model into a grade computed from what the gate found, which is the fusion T3's header
spends its first paragraph refusing.

DECLARED DATA WITH A LOCK, WHICH IS THE FORK THE PHASE PLAN TOOK AND WHY. The master spec
asks for "blast-radius por heuristica (manifest estatico de tabelas downstream), nao lineage
dinamico". Static is right: runtime lineage on Free Edition is a system-table dependency
with its own retention, for a question whose answer changes about once a phase. But a static
manifest that nothing checks is a list that goes stale silently, which is the failure mode
that makes people distrust the first decision -- so the manifest is data HERE and a test
holds it against the bundle, exactly as `opl.dataops.cadence` ships a cadence no schedule can
be derived from and `incidents.TABLE_OF_JOB` ships a job-to-table map the YAMLs are swept
for. A number or a name a human typed is strictly better than the same value typed into a
dashboard for one reason -- here it is in the diff -- and what makes it SAFE rather than
merely honest is that it cannot go stale without a commit turning red.

REJECTED, AND NEITHER IS REOPENED HERE: dynamic lineage (scope the spec refuses), and an
unlocked static list. Also rejected, for `incidents.py`'s reason and not a new one: reading
`databricks/resources/` at run time. `[tool.hatch.build.targets.wheel] packages =
["src/opl"]`, so the bundle is not in the artefact -- a module that read it would work in
this repository and raise in the only place it matters. The YAMLs are read by TESTS.

THE GRAPH HAS THREE LEGS AND THEY ARE ATTESTED BY THREE DIFFERENT THINGS. Saying which is
which is the whole of this module's honesty, because a lock over one leg reads, from the
outside, exactly like a lock over all of them.

  1. BRONZE -> VAULT IS DECLARED HERE AND THE BUNDLE ATTESTS IT. `VAULT_LOADS_FROM` below is
     held equal, in both directions, to every vault loader task the bundle declares --
     nineteen of them across five job files today. A new loader, a renamed table or a task
     repointed at another source fails `tests/triage_agent/test_blast_radius_lock.py` in
     the commit that does it.
  2. VAULT -> GOLD IS NOT IN THE BUNDLE AND IS NOT DECLARED HERE EITHER. The gold job YAMLs
     hand their tasks ONLY the gold table's name -- there is no source parameter to sweep --
     so the bundle cannot attest this leg at all. What can is the gold registry: an SCD2
     dimension declares `source_satellite`, a calendar dimension declares
     `applied_date_source`, a PIT declares `hub` and `satellites`, and those are the very
     fields `databricks/src/gold_load_*.py` resolve into the tables they read. So this leg
     is DERIVED from the registry rather than retyped (`vault_sources_of`), which is one
     spelling instead of two, and `_assert_every_gold_field_naming_a_vault_table_is_read`
     refuses a future gold kind that adds a source field the derivation does not look at.
  3. BRONZE -> GOLD, WITH NO VAULT TABLE IN BETWEEN, IS DECLARED AND IS THE LEG THIS FILE
     EXISTS FOR. Two bronze tables reach gold without a vault loader task anywhere in the
     bundle, and one of them is `payments` -- the table of `592660596679630`, the largest
     incident in this workspace and the one whose recorded recommendation is DO NOT PROMOTE.

WHY LEG 3 IS THE DANGEROUS ONE, STATED IN THE SHAPE IT WOULD HAVE ARRIVED IN. A manifest
built by walking bronze -> vault -> gold returns EMPTY for `payments` and for `ptax`. Empty
raises nothing. Empty is a perfectly plausible answer for a bronze table -- THREE of the
seven registered tables (lookup, merchant, socios) reach no gold table at all, so a short
answer is not even unusual here. (This paragraph said FIVE until the answers were measured.
Five is the number with no DIRECT gold edge -- a different set, and the only one of the two
that contains NEITHER of the tables this leg exists for.) And empty is the most reassuring
answer available, printed against the incident that most needs the opposite. That is this
phase's species in its purest form, and the defence is not a comment:
`_assert_no_bronze_table_reaches_nothing` runs at IMPORT and refuses a registered bronze
table whose whole downstream set is empty, so deleting `ptax` from `DIRECT_TO_GOLD`
breaks the import of every module that reads this one rather than quietly returning `()`.
The test that proves it fires drives the guard on a declaration missing that entry; it does
not merely observe that the entry is present.

WHAT NEITHER LEG ATTESTS, AND IT IS ONE SENTENCE LONG. `databricks/src/gold_load_fact.py`
reads bronze payments and bronze ptax, and `databricks/src/gold_load_conformed_dimension.py`
reads bronze payments -- and `tests/triage_agent/test_blast_radius_lock.py` sweeps those
entry points with `ast` and holds `DIRECT_TO_GOLD`'s KEY SET equal to the bronze tables they
read, so the leg is not unattested wholesale. What is a human's reading and nothing else is
WHICH GOLD TABLE each of those reads belongs to: `load_conformed_dimension` is handed the
same `fact_table` for all three conformed dimensions, and only `dim_date` USES it --
`covered_span` derives the calendar's span from it, while `dim_channel` and `dim_currency`
read it solely to REPORT `fact_side_cardinality`, a measurement that changes no row they
write. So `payments` is declared to reach `dim_date` and not the other two. That distinction
is in the body of one function, no test in this repository holds it, and if it is wrong the
wrong answer is two extra table names rather than a missing one.

TWO GOLD TABLES ARE IN NO BLAST RADIUS AT ALL AND THAT IS DECLARED RATHER THAN LEFT TO BE
NOTICED. `dim_channel` and `dim_currency` are written from `opl.contracts.payments`'
declared value domains, so no bronze row can change their contents. An unreachable gold
table is exactly what a stale vault->gold derivation also looks like, which is why the two
are named in `GOLD_WITHOUT_A_BRONZE_SOURCE` with their reasons and
`_assert_the_unreached_gold_tables_are_the_declared_ones` holds that set EQUAL to what the
graph does not reach. A gold table added tomorrow with no source the derivation can see
appears in that difference and fails the import.

THE THREE THINGS DELIBERATELY OUTSIDE THE GRAPH, each because including it would make every
answer the same answer:

  * THE FOUR DATAOPS VIEWS, AND THEY ARE NOT ALL OUT FOR THE SAME REASON -- which was
    measured off `opl.dataops.views.all_view_ddls` rather than read off their names.
    `dataops_reconciliation` and `dataops_reconciliation_by_file` read all seven bronze
    tables plus staging and quarantine, and `dataops_freshness` reads all seven: those three
    ARE downstream in the literal sense, and are left out because they are the instruments a
    triager READS during the incident rather than products it damages -- a manifest naming
    them would name them for all seven tables and carry no information. The fourth,
    `dataops_task_telemetry`, is out on a stronger ground and not on that one: it reads
    `system.lakeflow.jobs`, `system.lakeflow.job_task_run_timeline` and
    `system.query.history`, and no bronze table at all, so it is not downstream of bronze in
    any sense.
  * `streaming_payments_managed_broker`. It is fed by a Kafka topic, not by any bronze
    table; it is parallel to this graph rather than below it.
  * THE QUARANTINE AND STAGING TABLES of the gated table itself. They are the incident's
    own evidence, and `evidence.py` owns them.

`hub_empresa` HAS TWO BRONZE PARENTS AND THE DECLARATION IS SHAPED SO THAT CANNOT BE LOST.
`VAULT_LOADS_FROM` is keyed by the VAULT table and its value is a TUPLE of bronze sources,
because the obvious spelling -- one bronze source per vault table -- silently keeps whichever
the reader saw last. It is not a hypothetical: `vault_empresa_job.yml` loads `hub_empresa`
from `empresas` and `vault_estabelecimento_job.yml` loads it again from `estabelecimentos`,
which `tests/test_vault_job_wiring.py` already records as the vault's one deliberate double
load. Lose the second parent and `estabelecimentos` stops reaching `dim_company` and
therefore stops reaching `fact_payment`: two table names disappear from an answer that is
still a list of plausible tables.

AND THE TASK KEY IS NOT THE VAULT TABLE NAME. In exactly one of the nineteen loader tasks
the two disagree -- `task_key: hub_empresa_from_estabelecimentos` loads `hub_empresa` -- so a
sweep keyed on `task_key` invents a vault table nothing declares and drops the real edge,
silently, on one edge out of nineteen. The authority is the PARAMETER, never the name of the
thing that carries it, which is `incidents.py`'s ruling about `fail_on_dq`'s own parameter
applied one layer down. The test does not describe that trap; it runs the naive reader beside
the right one and asserts the single edge it gets wrong.

NO SQL, AND NO SPARK SESSION, WHICH IS A DEPARTURE FROM THE FOUR SIBLING MODULES AND IS
ARGUED RATHER THAN ASSUMED. `incidents`, `evidence`, `severity` and `history` all emit SQL
because their inputs are in the workspace. This module's inputs are all in the wheel, and
there are seven possible questions rather than one per row, so a SQL rendering would be a
second spelling of this lookup in another language -- the thing this phase forbids -- kept in
step by nothing. A caller assembling an issue payload calls `blast_radius` or
`blast_radius_note` on the `source` column T1 already publishes. There is consequently no
prose reaching a SQL statement from here and no call to
`opl.dataops.freshness.sql_string_literal`; the day one appears, that is the function to use.

IMPORTING THIS MODULE IMPORTS pyspark's PYTHON PACKAGE AND STARTS NO JVM, which is said
because the test file claims a no-session seam and the two are different claims.
`opl.vault.domains` imports pyspark at module scope, so it arrives here transitively; what
costs 25-33 s in this suite is a SparkSession, and nothing here builds or asks for one.

AN INCIDENT WHOSE `source` IS NULL IS REFUSED, AND THE REFUSAL IS `evidence.py`'s RATHER
THAN A SECOND SPELLING OF IT. T1 emits a NULL `source` for a gate that fired on a job
`TABLE_OF_JOB` does not declare, on purpose, so a rename that reached the workspace and not
the repository stays visible. `_spec_of_incident` already refuses that row with the message
that says what to fix; inventing an empty blast radius for it would render a stale
declaration exactly like a bronze table with nothing downstream -- which is the same
mistake as the one the import guard above exists to make impossible.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields

from opl.bronze.registry import REGISTRY
from opl.gold.registry import REGISTRY as GOLD_REGISTRY
from opl.gold.specs import (
    CalendarDimension,
    GoldTable,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
)
from opl.triage_agent.evidence import _spec_of_incident
from opl.vault import domains

__all__ = [
    "DIRECT_TO_GOLD",
    "GOLD_WITHOUT_A_BRONZE_SOURCE",
    "VAULT_LOADS_FROM",
    "BlastRadius",
    "blast_radius",
    "blast_radius_note",
    "gold_sources_of",
    "vault_sources_of",
]

# Which BRONZE tables each vault table is loaded FROM. DECLARED, and held equal to the
# bundle's nineteen vault loader tasks by `tests/triage_agent/test_blast_radius_lock.py`
# -- see the header's leg 1 for why this is data here rather than a run-time read of the
# YAMLs.
#
# KEYED BY THE VAULT TABLE WITH A TUPLE OF SOURCES, NOT THE OTHER WAY ROUND. `hub_empresa`
# is loaded twice on purpose and a one-source-per-table mapping keeps whichever entry was
# written last, with no error and no NULL. The shape is the defence.
#
# The KEY is the loader task's FIRST parameter (a `opl.vault.domains` key) and the VALUES are
# its SECOND (a `opl.bronze.registry` key). NEITHER IS THE `task_key`, which disagrees with
# the vault table in one task out of nineteen.
VAULT_LOADS_FROM: dict[str, tuple[str, ...]] = {
    # THE ONE TABLE WITH TWO FEEDS, AND THE ONLY LINE IN THIS FILE WHERE THAT IS VISIBLE.
    # `vault_empresa_job.yml:hub_empresa` loads it from the empresas contract; then
    # `vault_estabelecimento_job.yml:hub_empresa_from_estabelecimentos` loads it again from
    # the eight-digit root of every establishment's CNPJ. Both are real edges: a company
    # that appears only as an establishment's parent enters the hub through the second.
    "hub_empresa": ("empresas", "estabelecimentos"),
    "sat_empresa_dados": ("empresas",),
    "hub_estabelecimento": ("estabelecimentos",),
    "sat_estabelecimento_dados": ("estabelecimentos",),
    "sat_estabelecimento_endereco": ("estabelecimentos",),
    "link_empresa_estabelecimento": ("estabelecimentos",),
    "hub_merchant": ("merchant",),
    "sat_merchant_dados": ("merchant",),
    # LOADED FROM `merchant` ALONE THOUGH IT LINKS A MERCHANT TO A COMPANY. The link's
    # company end is derived from the merchant row's own CNPJ, so the empresas contract is
    # not read -- and the authority for that is the task's parameter, not the table's name.
    "link_merchant_empresa": ("merchant",),
    "sat_eff_merchant_empresa": ("merchant",),
    "link_company_partner": ("socios",),
    "sat_eff_company_partner": ("socios",),
    # F2 WAVE 2, AND THE FIRST EDGE HERE WITH NO LOADER TASK BEHIND IT YET. `link_payment`
    # is registered by `opl.vault.domains.payments_domain` and loaded from bronze
    # `payments` by `vault_load_link.py` -- but the JOB that runs that task cannot be
    # deployed while the workspace 403s on every NEW job resource, so the YAML lands with
    # the task that owns `databricks/`. Until it does, FIVE tests in
    # `tests/triage_agent/test_blast_radius_lock.py` and `tests/test_vault_job_wiring.py`
    # are RED on this one line, deliberately -- not one, which is what this comment claimed
    # until the T1 review counted them. The five are the bundle-versus-declaration
    # comparisons; every OTHER red this line produced was a consequence of `BlastRadius`
    # having no shape for a table with both legs, and those are fixed here rather than
    # waiting for a YAML file that could never have repaired them.
    #
    # The alternative to declaring the edge at all is leaving the key out, and
    # `_assert_the_vault_declaration_is_total_over
    # _both_registries` then refuses AT IMPORT and takes every module that reads this one
    # with it. A declared edge the bundle has not caught up with is the loud failure; an
    # undeclared vault table is the silent one this guard exists for.
    "link_payment": ("payments",),
    "ref_cnae": ("lookup",),
    "ref_motivo": ("lookup",),
    "ref_municipio": ("lookup",),
    "ref_natureza_juridica": ("lookup",),
    "ref_pais": ("lookup",),
    "ref_qualificacao": ("lookup",),
}

# The bronze tables that reach GOLD with NO vault table in between, and the gold tables they
# reach. DECLARED, and the leg the bundle cannot attest -- the gold job YAMLs hand their
# tasks only the gold table's name.
#
# THIS IS THE ENTRY THAT KEEPS THE WORKSPACE'S LARGEST INCIDENT FROM READING AS HARMLESS.
# A manifest walked bronze -> vault -> gold answers "nothing downstream" for `ptax`, and for
# `payments` it now answers something WORSE THAN NOTHING: `link_payment`, a vault table that
# no gold table reads. `payments` is `592660596679630`'s table, so the incident this whole
# module exists for is the one whose answer degraded when the vault leg arrived.
#
# "NEITHER OF THESE TWO TABLES HAS A VAULT LOADER TASK" IS WHAT THIS COMMENT SAID, AND F2
# WAVE 2 FALSIFIED IT. `payments` has one. That is why `BlastRadius` carries `gold_direct`
# as its own leg: the two facts -- feeds a vault table, and drives a gold table straight
# from bronze -- are both true of `payments` now, and a model with room for only one of
# them reported the union under the wrong heading.
DIRECT_TO_GOLD: dict[str, tuple[str, ...]] = {
    # `fact_payment` reads bronze payments as its SOURCE (`gold_load_fact.py`'s
    # `source_table=`), one fact row per payment event. `dim_date` reads it too, and only
    # for its SPAN: `opl.gold.conformed.covered_span` takes the calendar's ends from the
    # payment instants and the satellite's applied dates together.
    #
    # `dim_channel` AND `dim_currency` ARE DELIBERATELY ABSENT, and the header says why at
    # length: the same `fact_table` is handed to all three conformed loads, but those two
    # write members from `opl.contracts.payments`' declared domains and read the fact only
    # to REPORT how many of their members it reaches. A bad payments batch moves that
    # reported number and not one row of either table.
    "payments": ("dim_date", "fact_payment"),
    # The FX series. `gold_load_fact.py` passes it as `fx_source_table=`, and every
    # `fx_rate`, `fx_rate_date_key` and `amount_brl` in the fact is resolved from it -- so a
    # gated PTAX batch reaches the star's only additive measure without touching the vault.
    "ptax": ("fact_payment",),
}

# The gold tables NO bronze table can reach, with the reason each is structural rather than
# an omission. Held EQUAL to what the graph does not reach by
# `_assert_the_unreached_gold_tables_are_the_declared_ones`.
#
# WHY THIS IS DECLARED AT ALL, GIVEN IT IS DERIVABLE. An unreachable gold table is what a
# CORRECT answer looks like here and it is also what a stale vault->gold derivation looks
# like -- a new gold kind whose source field `vault_sources_of` does not read drops out of
# every blast radius and out of nothing else. Writing the expected set down turns that from
# a silence into a failed import.
GOLD_WITHOUT_A_BRONZE_SOURCE: dict[str, str] = {
    "dim_channel": (
        "written from opl.contracts.payments.PAYMENT_METHODS, a declared value domain: no "
        "bronze row decides its members, so no bronze incident changes it"
    ),
    "dim_currency": (
        "written from opl.contracts.payments.CURRENCIES, for dim_channel's reason -- the "
        "contract's own prediction that a second currency would be a value change and not "
        "a schema change is what makes this a declaration rather than a projection"
    ),
}


@dataclass(frozen=True, kw_only=True)
class BlastRadius:
    """What one bronze table feeds. Frozen, keyword-only, and NAMES ONLY.

    THERE IS NO COUNT, NO PROPORTION AND NO SCORE ON THIS RECORD, which is the header's
    first argument in the shape a caller meets it: the two tuples ARE the answer, and any
    magnitude a reader wants comes from `evidence.py`, which counts rows that exist."""

    source: str
    vault: tuple[str, ...]
    gold: tuple[str, ...]
    # THE GOLD THIS TABLE REACHES WITHOUT PASSING THROUGH A VAULT TABLE, and it is a
    # SEPARATE FIELD rather than a subtraction from `gold` because the two legs are two
    # different facts and one of them cannot be recovered from the other: a gold table can
    # be reachable BOTH ways, so `gold - gold_direct` is not "the vault-reached gold" and
    # any reader computing it would get a smaller set than the truth.
    #
    # F2 WAVE 2 IS WHAT FORCED THIS FIELD TO EXIST. Before it, every bronze table had
    # exactly one leg -- `payments` and `ptax` reached gold directly with an empty vault
    # tuple, everything else reached gold only through the vault -- so ONE tuple plus a
    # boolean said everything and `bypasses_the_vault` could be spelled "the vault leg is
    # empty". `link_payment` made `payments` the first table with BOTH legs, and the old
    # model answered that state by reporting the union of the two and attributing all of
    # it to the vault leg: `blast_radius_note("payments")` said `fact_payment` was reached
    # "through" `link_payment`, which reaches no gold table at all. A false sentence in an
    # issue body a person acts on.
    gold_direct: tuple[str, ...]

    @property
    def bypasses_the_vault(self) -> bool:
        """This table reaches at least one gold table with no vault table in between.

        READ OFF THE DIRECT LEG, NOT OFF AN EMPTY VAULT LEG, and the difference is the
        whole reason `gold_direct` exists. The old spelling was `not vault and gold`, which
        answers FALSE for a table that has a vault loader AND drives a gold table straight
        from bronze -- exactly `payments` since F2 wave 2. The bypass is a property of the
        direct leg alone; whether a vault leg also exists is a different question, and
        conflating them made the one table this repository most often triages report that
        it does not bypass the vault while its fact was being loaded from bronze.

        A table that reaches nothing at all cannot hold (`_assert_no_bronze_table_reaches_
        nothing`), so this is not hiding that case."""
        return bool(self.gold_direct)

    def __post_init__(self) -> None:
        """The direct leg must be part of the whole, and this is why `gold` stays STORED.

        `gold` is the union of the two legs and is therefore redundant with them, which is
        the shape that drifts -- `opl.config`'s month rule is this repository's record of
        what two spellings of one value cost. It is kept stored anyway because `gold` is
        the field the issue payload carries and `opl.triage_agent.issue._radius_of`
        compares field-for-field, so turning it into a property would change a serialised
        format to remove a redundancy. **So the redundancy is CHECKED instead of removed:**
        a direct leg holding a table the whole set does not is a derivation that has come
        apart, and it fails here rather than in a sentence a triager reads."""
        stray = frozenset(self.gold_direct) - frozenset(self.gold)
        if stray:
            raise ValueError(
                f"blast radius for {self.source!r} reaches {sorted(stray)} directly and "
                f"not at all: `gold` is the union of both legs, so a direct-leg table "
                "missing from it means the two were computed from different declarations"
            )


def vault_sources_of(table: GoldTable) -> tuple[str, ...]:
    """Which VAULT tables the loader for `table` reads, from the spec's own fields.

    DERIVED, NOT DECLARED, and that is leg 2's whole point: `Scd2Dimension.source_satellite`
    is the very string `gold_load_dimension.py` resolves into `source_table=`, so reading it
    here is one spelling rather than two. `opl.gold.registry_guards` already resolves the
    same fields against the vault registry, which is why an unregistered name cannot reach
    this function.

    THE SCD2 ARM RETURNS THE HUB AS WELL, and it is in no field: `gold_load_dimension`
    resolves `domains.parent_hub(satellite)` and reads BOTH tables, because the dimension's
    natural key `cnpj_basico` lives in the hub and its payload in the satellite. A
    derivation over field values alone would miss it, which is why the guard below requires
    field values to be a SUBSET of this result rather than equal to it.

    A KIND WITH NO VAULT SOURCE FALLS THROUGH TO `()` -- true today for the two enumerated
    dimensions and for the fact, and the exact shape a new kind would go stale in, which is
    what `_assert_every_gold_field_naming_a_vault_table_is_read` refuses."""
    if isinstance(table, Scd2Dimension):
        satellite = domains.table_spec(table.source_satellite)
        return (domains.parent_hub(satellite).name, satellite.name)
    if isinstance(table, CalendarDimension):
        return (table.applied_date_source,)
    if isinstance(table, PointInTimeTable):
        return (table.hub, *table.satellites)
    return ()


def gold_sources_of(table: GoldTable) -> tuple[str, ...]:
    """Which OTHER GOLD tables the loader for `table` reads.

    The star's own edges, and they are why a blast radius is a closure rather than two
    lookups: `fact_payment` joins `dim_company`, so an incident on `empresas` reaches the
    fact through a dimension and not through any table it names itself.

    DECLARED BY THE FACT AND ALREADY GUARDED ELSEWHERE: `_assert_every_fact_reaches_every_
    dimension_this_star_holds` holds `conformed` equal to the registry's conformed set, so
    a dimension added without a key here fails that import rather than this derivation."""
    if isinstance(table, PaymentFact):
        return (table.company_dimension, *table.conformed)
    return ()


def _vault_reached(source: str, loads_from: Mapping[str, tuple[str, ...]]) -> tuple[str, ...]:
    """Every vault table `source` is a declared parent of, sorted.

    The declaration is inverted rather than stored both ways round: two dictionaries would
    be two spellings, and the one that goes stale is the one no import guard reads."""
    return tuple(sorted(table for table, parents in loads_from.items() if source in parents))


def _gold_reached(vault: frozenset[str], direct: Iterable[str]) -> tuple[str, ...]:
    """Every gold table reachable from `vault` or from `direct`, closed over the star.

    THE CLOSURE IS NOT DECORATION. `fact_payment` names no vault table at all, so without
    it an incident on `empresas` stops at `dim_company` and `dim_date` -- omitting the one
    table the whole layer exists to produce, which is also the only gold table `ptax` has
    ever been able to reach."""
    reached = {
        name
        for name, table in GOLD_REGISTRY.items()
        if vault & frozenset(vault_sources_of(table))
    }
    reached.update(direct)
    while True:
        added = {
            name
            for name, table in GOLD_REGISTRY.items()
            if name not in reached and reached & set(gold_sources_of(table))
        }
        if not added:
            return tuple(sorted(reached))
        reached |= added


def _radius(
    source: str,
    *,
    loads_from: Mapping[str, tuple[str, ...]],
    direct: Mapping[str, tuple[str, ...]],
) -> BlastRadius:
    """`blast_radius` with both declarations injectable, for the import guards and the tests.

    The seam `opl.gold.registry.build_registry` has and for its reason: a refusal that can
    only be driven over the live declaration is a refusal nobody has seen fail. The public
    entry point below takes one argument and no seam, so a caller cannot reach this by
    accident."""
    vault = _vault_reached(source, loads_from)
    reaches = direct.get(source, ())
    return BlastRadius(
        source=source,
        vault=vault,
        gold=_gold_reached(frozenset(vault), reaches),
        # THE DIRECT LEG IS THE SAME CLOSURE OVER NO VAULT TABLES, not the raw declaration:
        # a gold table reached straight from bronze pulls in whatever the star reaches from
        # it, so `fact_payment` direct from `payments` still carries `dim_date` behind it.
        # Running the same function with an empty vault set is what keeps the two legs
        # closed the same way -- a second, simpler expression here would be the second
        # spelling that drifts.
        gold_direct=_gold_reached(frozenset(), reaches),
    )


def blast_radius(source: str | None) -> BlastRadius:
    """What the bronze table of one incident feeds, or refuse.

    `source` IS T1's COLUMN AND MAY BE NULL, which is a real row of that feed rather than a
    caller's mistake -- and the refusal is `evidence._spec_of_incident`'s, imported rather
    than re-spelled, because there is exactly one thing to say about a gate that fired on a
    job the declaration does not know and it is already said there.

    KEYED ON `spec.name` RATHER THAN ON THE STRING PASSED IN, so the registry resolves the
    name once and this module and its import guards agree about what a table is called.
    `_assert_every_registered_bronze_table_answers_to_its_own_key` is what makes that the
    same string as the `REGISTRY` key the declarations are written in."""
    return _radius(
        _spec_of_incident(source).name, loads_from=VAULT_LOADS_FROM, direct=DIRECT_TO_GOLD
    )


def blast_radius_note(source: str | None) -> str:
    """One sentence a person reads, for the issue payload T6 assembles.

    THREE ARMS AND EVERY ONE OF THEM IS REACHED BY A REAL TABLE IN THIS WORKSPACE --
    `payments` and `ptax` take the first, `empresas` and `estabelecimentos` the second,
    `merchant`, `socios` and `lookup` the third. An arm no input can reach is the shape this
    repository hunts, so the test names a table for each.

    THE FIRST ARM SAYS THE BYPASS OUT LOUD instead of leaving an empty vault list to be
    read as an empty answer. That sentence is the whole reason this function exists rather
    than a caller formatting the tuples.

    FOUR ARMS SINCE F2 WAVE 2, and the new one is FIRST because it is the only arm whose
    absence produced a FALSE sentence rather than a missing one. `payments` now feeds
    `link_payment` AND drives `fact_payment` straight from bronze; with three arms it fell
    through to "feeds ... in the vault, and through them ... in gold", attributing every
    gold table it reaches to a vault table that reaches none of them. `link_payment` is
    read by nothing in gold -- that is this phase's declared gap -- so the arm has to name
    the two legs separately or lie about one of them."""
    radius = blast_radius(source)
    if radius.vault and radius.bypasses_the_vault:
        return (
            f"{radius.source} feeds {', '.join(radius.vault)} in the vault, AND reaches "
            f"{', '.join(radius.gold_direct)} in gold DIRECTLY from bronze, not through "
            "the vault. A downstream manifest walked bronze -> vault -> gold reports the "
            "vault leg and misses the direct one"
        )
    if radius.bypasses_the_vault:
        return (
            f"{radius.source} has NO vault loader task in the bundle and still reaches gold: "
            f"{', '.join(radius.gold)}. A downstream manifest walked bronze -> vault -> gold "
            "reports nothing for this table"
        )
    if radius.gold:
        return (
            f"{radius.source} feeds {', '.join(radius.vault)} in the vault, and through them "
            f"{', '.join(radius.gold)} in gold"
        )
    return (
        f"{radius.source} feeds {', '.join(radius.vault)} in the vault and NO gold table -- "
        "no dimension, fact or point-in-time table in this star reads any of them"
    )


def _names_in(table: GoldTable) -> frozenset[str]:
    """Every string this spec's own fields carry, one level into tuples.

    ONE LEVEL IS ENOUGH AND IS SAID SO RATHER THAN ASSUMED: the only nested values in this
    registry are `FactRole` and `DerivedMeasure` records, whose strings are key names,
    contract columns and additivity words -- none of which can be a vault or gold table
    name, because `_assert_no_gold_name_is_owned_by_another_layer` keeps the three
    namespaces disjoint and a contract column is not a table."""
    found: set[str] = set()
    for field in fields(table):
        value = getattr(table, field.name)
        if isinstance(value, str):
            found.add(value)
        elif isinstance(value, tuple):
            found.update(item for item in value if isinstance(item, str))
    return frozenset(found)


def _assert_every_registered_bronze_table_answers_to_its_own_key() -> None:
    """`REGISTRY[key].name == key`, because this module uses the two interchangeably.

    The declarations above are written in REGISTRY KEYS -- which is what a loader task's
    parameter is and what `table_spec` resolves -- and `blast_radius` keys on `spec.name`.
    They are the same string for all seven tables today; if they ever were not, the guards
    below would sweep one namespace and the lookup another, and every answer would be empty
    rather than wrong."""
    astray = {key: spec.name for key, spec in REGISTRY.items() if spec.name != key}
    if astray:
        raise ValueError(
            f"bronze tables whose registry key is not their name: {astray}. The blast-radius "
            "declarations are written in keys and the lookup resolves to names; a table that "
            "answers to two strings falls out of one of them silently"
        )


def _assert_the_vault_declaration_is_total_over_both_registries() -> None:
    """Every vault table has at least one bronze parent, and every parent is registered.

    TOTALITY OVER THE VAULT IS THE HALF THAT MATTERS: a vault table missing from this
    declaration is one that no bronze incident ever names, so the gold tables built on it
    fall out of every answer -- the same silence leg 3 is about, one layer along.

    EMPTY TUPLES ARE REFUSED SEPARATELY from missing keys, because `"hub_x": ()` satisfies
    a check on the KEY SET while meaning the opposite of what the key set implies."""
    declared, registered = set(VAULT_LOADS_FROM), set(domains.REGISTRY)
    orphans = sorted(table for table, parents in VAULT_LOADS_FROM.items() if not parents)
    unknown = {
        table: sorted(set(parents) - set(REGISTRY))
        for table, parents in VAULT_LOADS_FROM.items()
        if set(parents) - set(REGISTRY)
    }
    if declared != registered or orphans or unknown:
        raise ValueError(
            f"the vault load declaration is not total over both registries: vault tables "
            f"with no declared source {sorted(registered - declared)}, declared tables the "
            f"vault does not register {sorted(declared - registered)}, entries naming no "
            f"parent at all {orphans}, parents no bronze registry knows {unknown}"
        )


def _assert_the_direct_gold_declaration_names_registered_tables() -> None:
    """Both ends of leg 3 are names their own registry knows, and neither end is empty.

    A key here is a `opl.bronze.registry` key and a value a `opl.gold.registry` name; the
    guard exists because this is the one leg no sweep of the bundle can correct -- a typo in
    `"fact_payments"` would drop the star's only fact out of the workspace's largest
    incident and read as a table that simply is not reached."""
    unknown_source = sorted(set(DIRECT_TO_GOLD) - set(REGISTRY))
    unknown_target = sorted(
        {name for targets in DIRECT_TO_GOLD.values() for name in targets} - set(GOLD_REGISTRY)
    )
    empty = sorted(table for table, targets in DIRECT_TO_GOLD.items() if not targets)
    if unknown_source or unknown_target or empty:
        raise ValueError(
            f"the direct-to-gold declaration names tables no registry holds: unregistered "
            f"bronze sources {unknown_source}, unregistered gold targets {unknown_target}, "
            f"sources declaring no target {empty}. An entry with no target is the empty "
            "answer this declaration exists to make impossible"
        )


def _assert_no_bronze_table_reaches_nothing(
    loads_from: Mapping[str, tuple[str, ...]] | None = None,
    direct: Mapping[str, tuple[str, ...]] | None = None,
) -> None:
    """THE GUARD LEG 3 IS FOR. No registered bronze table's blast radius may be empty.

    A bronze table that reaches nothing is what a stale manifest returns and it is also the
    single most reassuring thing this module could print, so it is refused outright rather
    than reported. Removing `ptax` from `DIRECT_TO_GOLD` breaks the import of every module
    that reads this one, which is the difference between a manifest and a list.

    THAT SENTENCE NAMED `payments` UNTIL F2 WAVE 2 AND WAS FALSIFIED BY IT, which is worth
    recording rather than quietly re-pointing. `payments` demonstrated this guard only
    because it had no vault leg: with `link_payment` registered, dropping its
    `DIRECT_TO_GOLD` entry leaves the radius non-empty and this guard stays silent. **So
    what died was the DEMONSTRATION, not the guard** -- it still refuses a table that
    reaches nothing, and `ptax` is now the table with no vault leg that proves it. The
    dropped-edge case `payments` used to cover is held by
    `tests/triage_agent/test_blast_radius_lock.py`'s `ast` sweep, which compares
    `DIRECT_TO_GOLD`'s KEY SET against the bronze tables the gold entry points actually
    read -- a stronger check than this one, and the reason the coverage did not move when
    the demonstration did.

    ITERATES `REGISTRY` KEYS AND NOT `blast_radius`, deliberately: the public entry point
    resolves through `table_spec`, so a registry entry whose spec belongs to another table
    would be answered for that other table and this guard would see a non-empty result.

    BOTH DECLARATIONS ARE INJECTABLE for `build_registry`'s reason -- a refusal that can only
    be driven over data that already passes is a refusal nobody has watched fail."""
    edges = VAULT_LOADS_FROM if loads_from is None else loads_from
    reaches = DIRECT_TO_GOLD if direct is None else direct
    radii = [_radius(table, loads_from=edges, direct=reaches) for table in sorted(REGISTRY)]
    empty = [radius.source for radius in radii if not radius.vault and not radius.gold]
    if empty:
        raise ValueError(
            f"registered bronze tables with nothing downstream: {empty}. Either they are "
            "loaded into the vault -- declare it in VAULT_LOADS_FROM -- or they reach gold "
            "directly and belong in DIRECT_TO_GOLD, which is how `payments` reaches "
            "`fact_payment` with no vault table in between. An empty blast radius is the "
            "most reassuring wrong answer this module can give and is refused, not printed"
        )


def _assert_the_unreached_gold_tables_are_the_declared_ones() -> None:
    """The gold tables no bronze table reaches are exactly the ones declared unreachable.

    EQUALITY IN BOTH DIRECTIONS. A gold table that fell out of every blast radius because
    `vault_sources_of` does not read its kind looks identical, from the outside, to one whose
    contents no bronze row decides -- so the second is written down and the first fails here.
    An entry that becomes reachable fails too, which keeps the declaration from outliving its
    reason."""
    reached = {name for table in REGISTRY for name in blast_radius(table).gold}
    unreached = set(GOLD_REGISTRY) - reached
    if unreached != set(GOLD_WITHOUT_A_BRONZE_SOURCE):
        raise ValueError(
            f"the unreachable-gold declaration does not match the graph: gold tables no "
            f"bronze table reaches and nothing here explains "
            f"{sorted(unreached - set(GOLD_WITHOUT_A_BRONZE_SOURCE))}, declared unreachable "
            f"but reached {sorted(set(GOLD_WITHOUT_A_BRONZE_SOURCE) - unreached)}. The first "
            "list is what a gold kind whose source this module cannot see looks like"
        )


def _assert_every_gold_field_naming_a_vault_table_is_read() -> None:
    """Every vault name a gold spec carries in a field is one `vault_sources_of` returns.

    A SUBSET AND NOT AN EQUALITY, which is the one asymmetry in this file: the SCD2 arm adds
    the satellite's parent hub, which is in no field of any spec, so equality would refuse
    the correct answer. What is being refused is the other direction -- a gold kind that
    gains a source field the derivation does not look at, which is how leg 2 goes stale and
    is invisible in every output because the table simply stops being named."""
    missed = {}
    for name, table in GOLD_REGISTRY.items():
        carried = _names_in(table) & set(domains.REGISTRY) - set(vault_sources_of(table))
        if carried:
            missed[name] = sorted(carried)
    if missed:
        raise ValueError(
            f"gold specs naming a vault table that `vault_sources_of` does not return: "
            f"{missed}. Add the field to that function; until then every bronze incident on "
            "those vault tables reports a blast radius that stops one layer short"
        )


def _assert_every_gold_field_naming_a_gold_table_is_read() -> None:
    """The same refusal for the star's own edges, and it is a separate failure.

    `gold_sources_of` reads the fact's `company_dimension` and `conformed`; a kind that
    gains a gold source the closure does not follow truncates the radius at that table
    instead of dropping one, so the two guards fail differently and are not one check
    written twice. The spec's OWN name is excluded -- every gold table names itself."""
    missed = {}
    for name, table in GOLD_REGISTRY.items():
        carried = _names_in(table) & set(GOLD_REGISTRY) - {name} - set(gold_sources_of(table))
        if carried:
            missed[name] = sorted(carried)
    if missed:
        raise ValueError(
            f"gold specs naming another gold table that `gold_sources_of` does not return: "
            f"{missed}. The closure stops there, so every table below it falls out of every "
            "blast radius that should have reached it"
        )


_assert_every_registered_bronze_table_answers_to_its_own_key()
_assert_the_vault_declaration_is_total_over_both_registries()
_assert_the_direct_gold_declaration_names_registered_tables()
_assert_no_bronze_table_reaches_nothing()
_assert_the_unreached_gold_tables_are_the_declared_ones()
_assert_every_gold_field_naming_a_vault_table_is_read()
_assert_every_gold_field_naming_a_gold_table_is_read()
