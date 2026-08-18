"""`LinkEnd.key_from` -- the declared derivation F-DB added, and the guard that is the
only reason declaring it is better than hard-coding it in a second loader.

WHAT THIS FILE IS FOR. `link_candidates` reads every hub's business key from the columns
the hub is NAMED after -- its own docstring says so in capitals -- and `bronze_merchant`
carries `cnpj`, fourteen characters, where `hub_empresa` keys on `cnpj_basico` at width
8. So `link_merchant_empresa`'s empresa end is DERIVED. The plan asserted `load_link`
could write it; that was false, and the repair is not a third loader but a DECLARATION on
the end, checked against the hub it claims to key.

THE CHECK IS THE POINT, NOT THE FIELD. A `KeyPrefix` whose width disagrees with the hub's
own does not crash: it keys on a different-length root, produces a digest `load_hub` never
wrote, and the link joins to nothing while reporting exactly the right row count -- the
quietest wrong answer this layer gives, and the one `opl.vault.loading`'s module docstring
opens with. Every refusal below is a state that would otherwise ship as a plausible table.

NO SPARK HERE, DELIBERATELY, which is why this is its own module rather than more lines in
`test_registry.py` (758 of its 800) or in the merchant vault file. Every assertion is about
a spec or about `build_registry`, both of which run before a session exists -- so this
module costs its chunk one import, exactly as `test_observation_grain.py` does, and is in
the same chunk for that reason."""
from __future__ import annotations

import pytest

from opl.vault import domains
from opl.vault.registry import (
    BusinessKeyColumn,
    EffectivitySatellite,
    Hub,
    KeyPrefix,
    Link,
    LinkEnd,
    VaultDomain,
    build_registry,
    identity_columns_of,
    linked_hubs,
)

# A hub keyed on ONE component at a declared width, which is `hub_empresa`'s shape and
# the only shape a prefix can key: eight characters of a longer column.
_ROOT_HUB = Hub(
    name="hub_root",
    hash_key="hub_root_hk",
    business_keys=(BusinessKeyColumn(name="root", width=8),),
)
# The hub the derived end is joined TO, keyed on a column the source names outright.
_OWN_HUB = Hub(
    name="hub_own",
    hash_key="hub_own_hk",
    business_keys=(BusinessKeyColumn(name="own_id"),),
)


def _link(*, key_from) -> Link:
    """A two-ended link whose SECOND end declares `key_from` against `_ROOT_HUB`."""
    return Link(
        name="link_own_root",
        hash_key="link_own_root_hk",
        hubs=(
            LinkEnd(hub=_OWN_HUB.name),
            LinkEnd(hub=_ROOT_HUB.name, key_from=key_from),
        ),
    )


def _registry(link: Link, *hubs: Hub):
    return build_registry([VaultDomain(name="probe", tables=(*hubs, link))])


# --- the spec's own refusals, before any registry exists -----------------------------


def test_a_prefix_of_zero_characters_is_refused():
    """Zero pads every row to the empty string, which collapses the end onto one hash
    key -- the same family as `BusinessKeyColumn`'s width refusal, and refused there."""
    with pytest.raises(ValueError, match="positive integer"):
        KeyPrefix(column="cnpj", width=0)


def test_a_prefix_needs_a_source_column():
    with pytest.raises(ValueError, match="source column name"):
        KeyPrefix(column="  ", width=8)


def test_a_bare_key_prefix_is_refused_rather_than_iterated():
    """A `KeyPrefix` is not a `Sequence`, so this is a TypeError either way -- but the
    message names the positional matching, which is what makes the tuple necessary."""
    with pytest.raises(TypeError, match="POSITIONALLY"):
        LinkEnd(hub="hub_root", key_from=KeyPrefix(column="cnpj", width=8))


def test_an_empty_declaration_is_not_read_as_no_declaration():
    """`None` and `()` are kept apart on purpose: `None` means "the source names the
    hub's own columns", and reading an empty tuple as that default would turn a
    half-written spec into the pre-F-DB behaviour with nothing to see."""
    with pytest.raises(ValueError, match="EMPTY key_from"):
        LinkEnd(hub="hub_root", key_from=())


def test_an_end_without_a_declaration_reads_the_hubs_own_column_names():
    """The property that makes this field free to add: every end written before F-DB
    answers exactly what `link_candidates` already read."""
    assert LinkEnd(hub="hub_root").source_columns(_ROOT_HUB) == ("root",)


def test_a_declared_end_reads_the_columns_it_names():
    end = LinkEnd(hub="hub_root", key_from=(KeyPrefix(column="cnpj", width=8),))

    assert end.source_columns(_ROOT_HUB) == ("cnpj",)


# --- the whole-set guard -------------------------------------------------------------


def test_a_prefix_whose_width_disagrees_with_the_hub_is_refused_naming_the_hub():
    """THE REFUSAL THIS FIELD EXISTS FOR, and the one that has no other failure mode.

    A seven-character prefix of a fourteen-character CNPJ is a different key space: the
    digest is one `load_hub` never wrote, so every reference dangles -- and nothing
    fails, because a link's references are COMPUTED from the source rather than joined
    to the hub. The message names the hub because the width it must agree with is that
    hub's declaration, which is the file the reader has to open."""
    link = _link(key_from=(KeyPrefix(column="cnpj", width=7),))

    with pytest.raises(ValueError, match="hub_root"):
        _registry(link, _OWN_HUB, _ROOT_HUB)


def test_the_width_refusal_says_what_the_wrong_width_costs():
    link = _link(key_from=(KeyPrefix(column="cnpj", width=14),))

    with pytest.raises(ValueError, match="join to nothing without failing"):
        _registry(link, _OWN_HUB, _ROOT_HUB)


def test_a_declaration_with_the_wrong_number_of_components_is_refused():
    """Matched POSITIONALLY, so a shorter or longer list derives and hashes the wrong
    column. `opl.vault.loading._padded` would refuse it too -- inside Spark, several
    tasks into a job, naming a component count rather than the link."""
    link = Link(
        name="link_own_pair",
        hash_key="link_own_pair_hk",
        hubs=(
            LinkEnd(hub=_OWN_HUB.name),
            LinkEnd(
                hub="hub_pair",
                key_from=(KeyPrefix(column="cnpj", width=8),),
            ),
        ),
    )
    pair = Hub(
        name="hub_pair",
        hash_key="hub_pair_hk",
        business_keys=(
            BusinessKeyColumn(name="root", width=8),
            BusinessKeyColumn(name="ordem", width=4),
        ),
    )

    with pytest.raises(ValueError, match="POSITIONALLY"):
        _registry(link, _OWN_HUB, pair)


def test_a_hub_that_declares_no_width_cannot_be_reached_by_a_prefix():
    """`width=None` means "take the value as it is" -- a hub deliberately declining to
    claim a canonical form -- so there is no width for a prefix to agree WITH, and a
    prefix taken against it would be an independent claim about that form."""
    unpadded = Hub(
        name="hub_root",
        hash_key="hub_root_hk",
        business_keys=(BusinessKeyColumn(name="root"),),
    )
    link = _link(key_from=(KeyPrefix(column="cnpj", width=8),))

    with pytest.raises(ValueError, match="declares width None"):
        _registry(link, _OWN_HUB, unpadded)


def test_a_declaration_that_agrees_with_its_hub_registers():
    link = _link(key_from=(KeyPrefix(column="cnpj", width=8),))

    registry = _registry(link, _OWN_HUB, _ROOT_HUB)

    assert registry["link_own_root"].ends[1].key_from[0].column == "cnpj"


# --- what the declaration changes downstream -----------------------------------------


def test_the_links_grain_is_the_source_columns_and_not_the_hubs_names():
    """THE HALF THAT DECIDES WHETHER A WINDOW CAN EVER CLOSE. `identity_columns_of` is
    the observation grain an effectivity satellite on this link must be keyed on, and the
    ledger projects BRONZE to exactly these columns. Answering `root` here -- the hub's
    own name -- would key the ledger on a column the source does not carry, which raises
    inside a vault job several tasks past the registry."""
    link = _link(key_from=(KeyPrefix(column="cnpj", width=8),))
    registry = _registry(link, _OWN_HUB, _ROOT_HUB)

    assert identity_columns_of(link, linked_hubs(registry, link)) == ("own_id", "cnpj")


def _two_ends_on_one_hub() -> Link:
    """A link whose two IDENTIFYING ends read ONE source column.

    Distinct roles, so the reference columns are `left_hub_own_hk` and `right_hub_own_hk`
    and `build_registry`'s collision guard has nothing to say -- these tests must reach
    the identity guard rather than be caught on the way to it. Both ends still read
    `hub_own`'s business key `own_id`, so the identity is `('own_id', 'own_id')`."""
    return Link(
        name="link_own_own",
        hash_key="link_own_own_hk",
        hubs=(LinkEnd(hub=_OWN_HUB.name, role="left"),
              LinkEnd(hub=_OWN_HUB.name, role="right")),
    )


def test_a_link_gating_an_effectivity_satellite_may_not_repeat_an_identity_column():
    """AT `build_registry`, NOT SEVERAL TASKS INTO A JOB.

    `identity_columns_of` concatenates the identifying ends' source columns with no dedup,
    because neither end knows about the other. That tuple is the observation ledger's key,
    `ObservationGrain.__post_init__` refuses a repeated key column, and
    `effectivity._refuse_a_mismatched_link_grain` requires the grain to be exactly that
    tuple -- so the pair is unsatisfiable and the satellite can never be loaded. Found at
    grain-construction time the message names the GRAIN; found here it names the LINK,
    which is the table someone has to change."""
    eff = EffectivitySatellite(name="eff_x", parent="link_own_own", entry_column="opened")
    domain = VaultDomain(name="probe", tables=(_OWN_HUB, _two_ends_on_one_hub(), eff))

    with pytest.raises(ValueError, match="takes its identity over 'own_id' twice"):
        build_registry([domain])


def test_the_same_repeat_is_ALLOWED_on_a_link_with_no_effectivity_satellite():
    """THE CONTROL, and it is a shipped table rather than a hypothetical.

    The obvious form of the guard above -- no link may repeat an identity column, which is
    what the review that prompted it asked for -- is WRONG, and wrong against this
    repository: `link_empresa_estabelecimento` is HIERARCHICAL,
    `hub_estabelecimento`'s business key CONTAINS `hub_empresa`'s, and its identity is
    legitimately `('cnpj_basico', 'cnpj_basico', 'cnpj_ordem', 'cnpj_dv')`. Hashing the
    parent's key and then the child's compound key that contains it is what a hierarchy
    IS. Nothing keys a ledger on it, so the repeat costs nothing.

    Both halves are asserted because the permission has to be pinned rather than inferred
    from a missing test: without this, the guard tightens to the blanket form on the next
    edit and refuses a registered, correct table."""
    link = _two_ends_on_one_hub()
    registry = _registry(link, _OWN_HUB)

    assert identity_columns_of(link, linked_hubs(registry, link)) == ("own_id", "own_id")

    real = domains.table_spec("link_empresa_estabelecimento")
    assert domains.link_identity_columns(real) == (
        "cnpj_basico", "cnpj_basico", "cnpj_ordem", "cnpj_dv"
    ), "the hierarchical link this permission exists for no longer has the shape claimed"


def test_the_two_links_written_before_this_field_declare_nothing():
    """The claim that keeps this a generalisation rather than a migration: `key_from` is
    `None` on every end of both CNPJ links, so their digests and their grains are
    computed by exactly the code they were computed by before."""
    for name in ("link_empresa_estabelecimento", "link_company_partner"):
        link = domains.table_spec(name)
        assert [end.key_from for end in link.ends] == [None] * len(link.ends), name
