"""One well-formed spec per gold KIND, each buildable with a single field replaced -- so
every refusal in `test_registry.py` and `test_fact_spec.py` is reached by changing exactly
the thing it refuses.

NOT A TEST MODULE AND NOT A `conftest.py`, and both halves of that are deliberate. These are
plain constructors, not fixtures: a `pytest.fixture` cannot be called twice in one test with
two different overrides, which is what every sweep below does. And they moved out of
`test_registry.py` rather than being copied, because F-API Task 4 took that file to 945 lines
against the project's 800-line cap and the two files that came out of it need the SAME
factories -- a second copy of `_fact` is the drift this repository polices hardest, and a
factory whose two copies disagree makes each file's refusals a claim about a different spec.

WHY EVERY FACTORY POINTS AT REAL SOURCES. `build_registry` resolves `company_dimension`,
`conformed`, a PIT's hub and satellites, and a dimension's `source_satellite` against the
LIVE registries -- so a probe naming invented tables would be refused by the resolution guard
before the guard under test could fire, and a sweep would then be measuring the wrong refusal.
"""
from __future__ import annotations

from opl.contracts import payments
from opl.gold.registry import (
    CalendarDimension,
    DerivedMeasure,
    EnumeratedDimension,
    FactRole,
    PaymentFact,
    PointInTimeTable,
    Scd2Dimension,
)
from opl.gold.spec_fields import (
    ADDITIVE,
    ADDITIVE_WITHIN_CURRENCY,
    FROM_CONTRACT,
    READS_ISO_TEXT,
)


def _dimension(**overrides) -> Scd2Dimension:
    """A well-formed dimension, with one field replaced -- so every refusal below is
    reached by changing exactly the thing it refuses."""
    fields = {
        "name": "dim_probe",
        "surrogate_key": "probe_sk",
        "source_satellite": "sat_empresa_dados",
    }
    fields.update(overrides)
    return Scd2Dimension(**fields)


def _enumerated(**overrides) -> EnumeratedDimension:
    """A well-formed enumerated dimension, same shape and same purpose as `_dimension`."""
    fields = {
        "name": "dim_probe_enum",
        "surrogate_key": "probe_enum_key",
        "natural_key": "probe_code",
        "fact_column": payments.EVENT_TIME_COLUMN,
        "members": ("A", "B"),
    }
    fields.update(overrides)
    return EnumeratedDimension(**fields)


def _calendar(**overrides) -> CalendarDimension:
    fields = {
        "name": "dim_probe_date",
        "surrogate_key": "probe_date_key",
        "natural_key": "probe_full_date",
        "applied_date_source": "sat_empresa_dados",
        "roles": (
            FactRole(
                key="probe_event_date_key",
                fact_column=payments.EVENT_TIME_COLUMN,
                source=FROM_CONTRACT,
                reads=READS_ISO_TEXT,
            ),
        ),
    }
    fields.update(overrides)
    return CalendarDimension(**fields)


def _pit(**overrides) -> PointInTimeTable:
    """A well-formed point-in-time table, same shape and purpose as `_dimension`.

    ITS HUB AND SATELLITES ARE THE REAL ONES, because `build_registry` resolves them
    against the live vault registry -- a probe pointing at invented names would be refused
    by the PIT guard before the cross-layer guard under test could fire, and the sweep
    would then be measuring the wrong refusal."""
    fields = {
        "name": "pit_probe",
        "hub": "hub_estabelecimento",
        "satellites": ("sat_estabelecimento_dados", "sat_estabelecimento_endereco"),
        "as_of_column": "probe_as_of",
    }
    fields.update(overrides)
    return PointInTimeTable(**fields)


def _fact(**overrides) -> PaymentFact:
    """A well-formed payment fact, same shape and purpose as `_dimension`.

    ITS `company_dimension` AND `conformed` NAME THE REAL TABLES, for `_pit`'s reason: the
    whole-set guards resolve both against this registry, so a probe pointing at invented
    names would be refused by the fact guard before the cross-layer guard under test could
    fire, and the sweep would then be measuring the wrong refusal. The sweeps below hand
    `build_registry` a single spec, and the cross-layer and duplicate-name guards run
    FIRST -- which is the "individually wrong before collectively wrong" order
    `opl.bronze.registry` states and which is what keeps this factory usable there."""
    fields = {
        "name": "fact_probe",
        "grain_key": payments.IDENTITY_COLUMN,
        "measure": "amount",
        "measure_additivity": ADDITIVE_WITHIN_CURRENCY,
        "company_dimension": "dim_company",
        "roles": (
            ("payer_cnpj_basico", "probe_payer_sk"),
            ("payee_cnpj_basico", "probe_payee_sk"),
        ),
        # ONE DERIVED MEASURE AND NOT THE LIVE FACT'S TWO, because the probe needs exactly
        # what `_assert_exactly_one_measure_is_additive` demands and nothing else: with a
        # two-member currency domain the delivered measure cannot be the additive one, so a
        # probe declaring no derived measure at all would be refused before any guard under
        # test could fire.
        "derived": (
            DerivedMeasure(name="probe_amount_brl", inputs=("amount",), additivity=ADDITIVE),
        ),
        "conformed": ("dim_date", "dim_channel", "dim_currency"),
    }
    fields.update(overrides)
    return PaymentFact(**fields)


# Every kind the gold registry knows, with a factory that builds a well-formed one. The
# cross-layer guards below are parametrised over this rather than over `Scd2Dimension`
# alone: a name collision is a property of the NAME, so a guard that only saw one kind
# would be a guard the second kind walks past.
#
# `pit` IS THE KIND THAT PROVED THE ARGUMENT RATHER THAN ILLUSTRATING IT. It is the first
# entry here with no `surrogate_key` and no `fact_column`, and adding it turned
# `_assert_no_two_dimensions_draw_from_one_payment_column` into an `AttributeError` at
# import of every gold module -- that guard skipped `Scd2Dimension` and assumed everything
# else had a `fact_column`. A sweep parametrised over one kind would not have found it.
#
# `fact` IS THE FIFTH AND IT IS IN HERE FOR THE SAME REASON, CHECKED THE SAME WAY. It has
# no `fact_column` either, so it walks the same line the PIT broke; the difference is that
# this time the direction was checked before the kind was added rather than after, and the
# guard was already an inclusion.
KINDS = {
    "scd2": _dimension,
    "enumerated": _enumerated,
    "calendar": _calendar,
    "pit": _pit,
    "fact": _fact,
}

# HOW THE COLLIDING NAME IS SPELLED, and the second entry is the whole point. Unity
# Catalog and Spark resolve identifiers CASE-INSENSITIVELY, so `SAT_EMPRESA_DADOS` and
# `sat_empresa_dados` are ONE Delta table -- and the sweeps below iterate the registries'
# own strings, which are all lower case, so a byte comparison passes every one of them
# while the upper-cased spelling of the same table walks straight through. Measured
# before the guard was casefolded: `sat_empresa_dados` refused, `SAT_EMPRESA_DADOS`
# ACCEPTED, same for `ref_pais` and `bronze_cnpj_empresas`. It is the exact defect
# `opl.bronze.registry_collisions` fixed in F1.4b, reintroduced at the one boundary no
# other file polices.
SPELLINGS = {"as declared": str, "upper-cased": str.upper}

