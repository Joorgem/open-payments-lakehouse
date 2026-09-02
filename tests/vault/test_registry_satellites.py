"""WHAT A SATELLITE'S PARENT MAY BE -- `opl.vault.registry_satellites`' three whole-set
guards, driven through `build_registry`. No Spark.

WHY A FILE OF ITS OWN, AND IT IS THE SAME SEAM THE SOURCE TOOK. F2 wave 2 widened the
parent from `Hub` to `Hub | Link`, which is the largest change any guard in this registry
has had: one refusal lifted, two pairings added, a new collision surface argued and
dismissed.

THE NUMBER THIS PARAGRAPH USED TO CARRY WAS NOT DERIVABLE AND IS REPLACED BY ONE THAT IS.
It said driving the change "cost `tests/vault/test_registry.py` 158 lines" -- a figure for
a file that was never written that way, and one that does not reconcile with the split
that actually happened. Measured instead: `git show cae3eff:tests/vault/test_registry.py |
wc -l` is **781**, against a strictly-under-800 cap. Nineteen lines of room, for a change
whose tests are this whole file. Master protocol section 4.12 is that whoever touches a
file at the cap splits it FIRST, and `src/opl/vault/registry_satellites.py` split at
exactly this subject, so the tests follow it rather than inventing a second boundary.

WIDENING A REFUSAL IS HOW A GUARD STOPS BEING ABLE TO FAIL, WHICH IS WHAT THIS FILE IS
FOR. The guard used to say "not a hub" and now says "not a hub and not a link", and the
cheap way to write that is `isinstance(parent, Hub | Link)` -- which is what it does. What
that could quietly have admitted is everything else: a satellite parented on another
satellite, on an EFFECTIVITY satellite (which is a satellite on a link in every sense but
the type's), or on a reference table (which declares no hash key at all, deliberately).
Each is driven below, one test each, so the widening cannot be reported by the tests that
exercise what it was widened FOR.

AND THE TWO PAIRINGS ARE DRIVEN IN BOTH DIRECTIONS. `Satellite.transactional` decides
whether an observation ledger gates the table, and an optional gate is one a later table
forgets to switch on -- so a transactional satellite on a HUB and a state satellite on a
LINK are both refused, and both refusals name what would have to change rather than
stating a rule. A file that drove only the first would leave the field looking like a way
to opt out of a ledger.

THE FIXTURE SPECS COME FROM `test_registry.py`, imported rather than rebuilt: `_HUB`,
`_SAT`, `_LINK` and `_domain` are that file's throwaway registry and are what every
refusal in this package is driven against. Two copies would be two things to keep in step,
and the divergence would show up as a guard passing here and failing there."""
from __future__ import annotations

import pytest

from opl.vault.columns import APPLIED_DATE, HASH_DIFF
from opl.vault.loading import applied_date_expression
from opl.vault.registry import (
    AppliedDateSource,
    EffectivitySatellite,
    ReferenceTable,
    Satellite,
    build_registry,
    parent_hub,
    parent_of,
)
from opl.vault.satellites import _expected_source_type
from opl.vault.specs import READS_ISO_TEXT

from .test_registry import _HUB, _LINK, _OTHER_HUB, _SAT, _domain


def test_a_satellite_whose_parent_is_not_registered_is_refused():
    """Across domains as well as within one: `build_registry` sees every domain at
    once, so this refusal does not depend on the order the filesystem yielded the
    modules in.

    THE MATCH IS ON THE MISSING-PARENT MESSAGE AND NOT ON THE PARENT'S NAME, WHICH IS A
    CORRECTION THIS TASK'S MUTATION RUN FORCED. It read `match="hub_thing"`, and BOTH
    refusal arms print that string -- so deleting the `parent is None` arm outright left
    this test GREEN: `isinstance(None, Hub | Link)` is false, the kind arm fired instead,
    and its message names the parent too. A lock that cannot tell which of two guards
    answered is a lock that reports the survivor. The message text below is unique to the
    arm this test is about, and the second assertion pins that the OTHER arm's wording is
    not what came back."""
    with pytest.raises(ValueError, match="which no domain registers") as raised:
        build_registry([_domain(_SAT)])

    assert "hub_thing" in str(raised.value)
    assert "not a hub and not a link" not in str(raised.value)


def test_a_satellite_whose_parent_is_another_satellite_is_refused():
    """A satellite hangs off a hub or a link, never off another satellite. Without
    this the parent lookup would succeed and the satellite would key on a column its
    'parent' does not have.

    THE GUARD WIDENED IN F2 WAVE 2 AND THIS IS ONE OF THE THREE TESTS THAT SAY IT DID NOT
    WIDEN TOO FAR. `assert_every_satellite_hangs_off_a_hub_or_a_link` now admits a `Link`,
    and widening a refusal is how a guard stops being able to fail -- so the `isinstance`
    is written against the two kinds that HAVE a hash key rather than against the kinds it
    used to name, and a satellite, an effectivity satellite and a reference table are all
    still refused. This test drives the first; the two below drive the others."""
    other = Satellite(name="sat_other", parent="sat_thing_dados", payload_columns=("x",))

    with pytest.raises(ValueError, match="not a hub and not a link"):
        build_registry([_domain(_HUB, _SAT, other)])


def test_a_satellite_whose_parent_is_an_effectivity_satellite_is_refused():
    """The second kind the widened guard must still refuse, and the one an `isinstance`
    against `Hub | Link` could most plausibly have let through by accident -- an
    effectivity satellite IS a satellite on a link in every sense but the type's.

    It has no hash key of its own: it KEYS on its parent link's, so a satellite pointed at
    one would take `EffectivitySatellite.hash_key` and there is no such attribute. That is
    an `AttributeError` inside `build_registry` rather than a refusal, which is why the
    guard names the two admitted kinds rather than excluding the ones it has met."""
    effectivity = EffectivitySatellite(
        name="sat_eff_thing", parent="link_thing_other", entry_column="entered_on"
    )
    on_effectivity = Satellite(
        name="sat_on_effectivity", parent="sat_eff_thing", payload_columns=("x",)
    )

    with pytest.raises(ValueError, match="not a hub and not a link"):
        build_registry(
            [_domain(_HUB, _OTHER_HUB, _LINK, effectivity, on_effectivity)]
        )


def test_a_satellite_whose_parent_is_a_reference_table_is_refused():
    """The third kind, and the one whose refusal is a statement about `ReferenceTable`'s
    own decision: it declares NO hash key at all, deliberately, because its natural key
    already is the column anything would join on. A satellite parented on one has nothing
    to key on."""
    on_reference = Satellite(
        name="sat_on_reference", parent="ref_thing", payload_columns=("x",)
    )
    reference = ReferenceTable(
        name="ref_thing", lookup_type="thing", natural_key="codigo", payload="descricao"
    )

    with pytest.raises(ValueError, match="not a hub and not a link"):
        build_registry([_domain(_HUB, reference, on_reference)])


def test_a_descriptive_satellite_whose_parent_is_a_link_is_admitted():
    """THE REFUSAL THIS TEST USED TO DRIVE IS GONE, AND ITS OWN MESSAGE NAMED THE
    CONDITION: "one parented on a LINK -- which DV2 does allow -- would be a registered
    table nothing in this package can write. The guard and that signature have to change
    together." F2 wave 2 changed both in one task, and `sat_link_payment` is the table
    that consumed it.

    THE TEST IS RESTATED RATHER THAN DELETED, because the interesting assertion is the
    same one pointed the other way: a link-parented satellite must now BUILD, and must
    resolve to its link through `parent_of` while `parent_hub` still refuses it. The
    second half is what keeps the widening from reaching `opl.gold.registry_guards`,
    which calls `parent_hub` for every SCD2 dimension's source and reads
    `business_key_columns` off the answer.

    IT MUST DECLARE `transactional`, which is not incidental to this test: the pairing
    guard refuses a STATE satellite on a link, because `load_satellite` has no link-grain
    ledger comparison. `test_a_state_satellite_on_a_link_is_refused_naming_what_is_
    missing` drives that arm."""
    on_link = Satellite(
        name="sat_on_link", parent="link_thing_other", payload_columns=("x",),
        transactional=True,
    )

    registry = build_registry([_domain(_HUB, _OTHER_HUB, _LINK, on_link)])

    assert parent_of(registry, registry["sat_on_link"]) is registry["link_thing_other"]
    with pytest.raises(ValueError, match="not a\n?\\s*hub"):
        parent_hub(registry, registry["sat_on_link"])


def test_a_state_satellite_on_a_link_is_refused_naming_what_is_missing():
    """THE FIRST OF THE TWO PAIRINGS THE WIDENED GUARD STILL REFUSES, and it is a
    DEFERRAL with a named consumer rather than a rule.

    A satellite that does not declare `transactional` is gated on an observation ledger,
    and for a link parent that ledger has to be keyed on the LINK's identity columns and
    read through the link's own prefixes. The comparison that checks that is
    `opl.vault.effectivity._refuse_a_mismatched_link_grain`, written for the effectivity
    satellite; `load_satellite` does not call it. Registered without this refusal, such a
    table would reach `satellite_grain._grain_key_mismatch`, which reads
    `business_key_columns` off its parent -- an `AttributeError` on a `Link`, several
    frames from the declaration that caused it."""
    state_on_link = Satellite(
        name="sat_state_on_link", parent="link_thing_other", payload_columns=("x",)
    )

    with pytest.raises(ValueError, match="_refuse_a_mismatched_link_grain"):
        build_registry([_domain(_HUB, _OTHER_HUB, _LINK, state_on_link)])


def test_an_event_satellite_on_a_hub_is_refused_so_the_flag_cannot_switch_a_ledger_off():
    """THE SECOND PAIRING, AND THE ONE THAT KEEPS `transactional` FROM BEING A SWITCH.

    Every hub in this vault is keyed on a business object observed in snapshots, so its
    keys really can be absent from a later one -- which is the single state the vault's
    end-dating path reads. Declaring a satellite on a hub transactional would drop the
    departure count AND `observation._window`'s refusal of a month nothing ever loaded,
    silently, since neither shows up in the rows written. Without this arm the field would
    be exactly the "relax the lock to get green" move it was added to make impossible."""
    event_on_hub = Satellite(
        name="sat_thing_events", parent="hub_thing", payload_columns=("x",),
        transactional=True,
    )

    with pytest.raises(ValueError, match="transactional=True and hangs off hub"):
        build_registry([_domain(_HUB, event_on_hub)])


def test_a_satellites_applied_date_source_colliding_with_its_parents_hash_key_is_refused():
    """The payload is not the only column read out of the source by name: the
    applied-date source is too, and the loader writes the parent's digest into the
    parent's hash key. Declared as one, the value read would be replaced by the write.

    ONLY VISIBLE ONCE THE PARENT IS RESOLVED, exactly like the payload collision beside
    it, which is why this is a whole-set guard and not an `AppliedDateSource`
    `__post_init__` check -- that one refuses the four METADATA column names, which are
    knowable in isolation, and knows nothing about which hub this satellite hangs off."""
    clash = Satellite(
        name="sat_clash_applied", parent="hub_thing", payload_columns=("colour",),
        applied_date_from=AppliedDateSource(column="hub_thing_hk"),
    )

    with pytest.raises(ValueError, match="its applied-date source"):
        build_registry([_domain(_HUB, clash)])


def test_a_satellite_payload_colliding_with_its_hubs_hash_key_is_refused():
    """Only visible once the parent is resolved, so it is a whole-set guard rather
    than a `__post_init__` one."""
    clash = Satellite(
        name="sat_clash", parent="hub_thing", payload_columns=("hub_thing_hk",)
    )

    with pytest.raises(ValueError, match="hub_thing_hk"):
        build_registry([_domain(_HUB, clash)])


# --------------------------------------------------------------------------- #
# `AppliedDateSource` -- the per-table half, refused at construction, before any
# registry exists and before Spark.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"column": "  "}, "needs a column name"),
        ({"column": "event_time", "reads": "iso"}, "not one of"),
        ({"column": APPLIED_DATE}, "the loaders write that themselves"),
        ({"column": HASH_DIFF}, "the loaders write that themselves"),
    ),
)
def test_an_applied_date_source_is_refused_at_construction(kwargs, message):
    """THREE MISTAKES, ALL CHECKABLE ABOUT ONE DECLARATION IN ISOLATION, which is why they
    are here and not in `build_registry`.

    A BLANK COLUMN would be read as `F.col('  ')` and fail inside Spark naming a column
    nobody typed. A READER OUTSIDE THE CLOSED SET has no expression behind it, and
    `opl.vault.loading.applied_date_expression` refuses rather than falling through -- but
    refusing there means refusing several frames into a load, where refusing here means
    refusing at import of the domain module. A METADATA COLUMN is the quiet one: the loader
    writes `applied_date` itself, so a source column of that name would be read and then
    overwritten by the value read from it, which is harmless and reads exactly like a
    declaration that took effect.

    `reads='iso'` IS THE MISSPELLING THAT MATTERS, not a nonsense string: the constant is
    `'iso-instant-text'`, and a near-miss is what a hand-written declaration produces."""
    with pytest.raises(ValueError, match=message):
        AppliedDateSource(**kwargs)


def test_a_satellite_handed_a_bare_column_name_as_its_applied_date_source_is_refused():
    """A bare `str` cannot carry the RULE for reading a day out of the column, and the two
    shapes this vault has need different expressions: `_snapshot_ref_date` is already a
    `date` and `event_time` is a 24-character ISO string. Accepted, the string would reach
    `applied_date_expression` and fail on `.reads` -- an `AttributeError` several frames
    from the declaration."""
    with pytest.raises(TypeError, match="not an AppliedDateSource"):
        Satellite(
            name="sat_x", parent="hub_thing", payload_columns=("colour",),
            applied_date_from="event_time",
        )


def test_the_expression_builder_refuses_a_reader_it_has_no_branch_for():
    """THE ARM `AppliedDateSource` MAKES UNREACHABLE BY CONSTRUCTION, driven anyway --
    which is the only way to know it can fire at all.

    The reader is validated against a closed set at construction, so no ordinary caller
    can reach this branch; `object.__setattr__` past the frozen dataclass is what a FUTURE
    EDIT looks like from here -- somebody adds a third reader to `APPLIED_DATE_READERS`
    and not to the expression. What must not happen then is a fallthrough returning
    `F.col(...)`: that would read a 24-character string as a date column and hand
    `changed_rows` a NULL ordering key for every row, ordering the whole version chain
    arbitrarily with nothing failing.

    THE MESSAGE NAMES THE PAIR THAT HAS TO MOVE TOGETHER, because that is the only useful
    thing to say to whoever gets here."""
    smuggled = AppliedDateSource(column="event_time", reads=READS_ISO_TEXT)
    object.__setattr__(smuggled, "reads", "microseconds")

    with pytest.raises(ValueError, match="has to move together"):
        applied_date_expression(smuggled)


def test_the_loaders_type_table_refuses_a_reader_it_has_no_entry_for():
    """THE SAME ARM ONE LAYER ALONG, AND IT IS THE ONE THAT ACTUALLY FIRES.

    `opl.vault.satellites._APPLIED_DATE_TYPES` answers the same question about the same
    closed set as `applied_date_expression` above -- which Spark type each reader needs --
    and it was TOTAL BY `KeyError`, while its neighbour's own docstring makes "total by
    refusal and not by a fallback" the rule. A third reader added to
    `APPLIED_DATE_READERS` and not to that dict raised a bare `KeyError` on a string, in a
    traceback showing a dict subscript and naming neither the set nor the satellite.

    AND THE ORDER IS WHY THIS MATTERS MORE THAN THE ARM ABOVE. `satellite_candidates`
    calls `_refuse_an_applied_date_the_source_cannot_provide` BEFORE it builds any
    expression, so through `load_satellite` this refusal is reached FIRST and
    `applied_date_expression`'s is unreachable -- that one is driven only by the test
    above calling the builder directly. The pair's stated rule held on the path nothing
    takes and not on the path every load takes.

    `object.__setattr__` PAST THE FROZEN DATACLASS is what a future edit looks like from
    here, for the reason the test above gives."""
    smuggled = AppliedDateSource(column="event_time", reads=READS_ISO_TEXT)
    object.__setattr__(smuggled, "reads", "microseconds")

    with pytest.raises(ValueError, match="_APPLIED_DATE_TYPES"):
        _expected_source_type(smuggled)
